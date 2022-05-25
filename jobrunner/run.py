"""
Script which polls the database for active (i.e. non-terminated) jobs, takes
the appropriate action for each job depending on its current state, and then
updates its state as appropriate.
"""
import datetime
import logging
import random
import shlex
import sys
import time
from typing import Optional

from jobrunner import config
from jobrunner.executors import get_executor_api
from jobrunner.job_executor import (
    ExecutorAPI,
    ExecutorState,
    JobDefinition,
    Privacy,
    Study,
)
from jobrunner.lib.database import find_where, select_values, update
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.models import Job, State, StatusCode
from jobrunner.project import requires_db_access
from jobrunner.queries import calculate_workspace_state, get_flag_value


log = logging.getLogger(__name__)


class InvalidTransition(Exception):
    pass


def main(exit_callback=lambda _: False):
    log.info("jobrunner.run loop started")
    api = get_executor_api()

    while True:
        active_jobs = handle_jobs(api)

        if exit_callback(active_jobs):
            break

        time.sleep(config.JOB_LOOP_INTERVAL)


def handle_jobs(api: Optional[ExecutorAPI]):
    log.debug("Querying database for active jobs")
    active_jobs = find_where(Job, state__in=[State.PENDING, State.RUNNING])
    log.debug("Done query")
    # Randomising the job order is a crude but effective way to ensure that a
    # single large job request doesn't hog all the workers. We make this
    # optional as, when running locally, having jobs run in a predictable order
    # is preferable
    if config.RANDOMISE_JOB_ORDER:
        random.shuffle(active_jobs)

    for job in active_jobs:
        # `set_log_context` ensures that all log messages triggered anywhere
        # further down the stack will have `job` set on them
        with set_log_context(job=job):
            handle_single_job(job, api)

    return active_jobs


# we do not control the tranisition from these states, the executor does
STABLE_STATES = [
    ExecutorState.PREPARING,
    ExecutorState.EXECUTING,
    ExecutorState.FINALIZING,
]


def handle_single_job(job, api):
    mode = get_flag_value("mode")
    paused = get_flag_value("paused", "").lower() == "true"
    try:
        handle_job(job, api, mode, paused)
    except Exception:
        mark_job_as_failed(
            job,
            "Internal error: this usually means a platform issue rather than a problem "
            "for users to fix.\n"
            "The tech team are automatically notified of these errors and will be "
            "investigating.",
        )
        # Do not clean up, as we may want to debug
        #
        # Raising will kill the main loop, by design. The service manager
        # will restart, and this job will be ignored when it does, as
        # it has failed. If we have an internal error, a full restart
        # might recover better.
        raise


def handle_job(job, api, mode=None, paused=None):
    """Handle an active job.

    This contains the main state machine logic for a job. For the most part,
    state transitions follow the same logic, which is abstracted. Some
    transitions require special logic, mainly the initial and final states, as
    well as supporting cancellation.
    """
    assert job.state in (State.PENDING, State.RUNNING)
    definition = job_to_job_definition(job)

    if job.cancelled:
        # cancelled is driven by user request, so is handled explicitly first
        # regardless of executor state.
        api.terminate(definition)
        mark_job_as_failed(job, "Cancelled by user", StatusCode.CANCELLED_BY_USER)
        api.cleanup(definition)
        return

    # handle special modes beofre considering executor state, as they ignore it
    if paused:
        if job.state == State.PENDING:
            # do not start the job, keep it pending
            return

    if mode == "db-maintenance" and definition.allow_database_access:
        if job.state == State.RUNNING:
            log.warning(f"DB maintenance mode active, killing db job {job.id}")
            # we ignore the JobStatus returned from these API calls, as this is not a hard error
            api.terminate(definition)
            api.cleanup(definition)

        # reset state to pending and exit
        set_state(job, State.PENDING, "Waiting for database to finish maintenance")
        return

    initial_status = api.get_status(definition)

    # handle the simple no change needed states.
    if initial_status.state in STABLE_STATES:
        if job.state == State.PENDING:
            log.warning(
                f"state error: got {initial_status.state} for a job we thought was PENDING"
            )
        # no action needed, simply update job message and timestamp
        message = initial_status.state.value.title()
        set_message(job, message)
        return

    # ok, handle the state transitions that are our responsibility
    if initial_status.state == ExecutorState.UNKNOWN:
        # a new job
        if job.state == State.RUNNING:
            log.warning(
                "state error: got UNKNOWN state for a job we thought was RUNNING"
            )

        # check dependencies
        awaited_states = get_states_of_awaited_jobs(job)
        if State.FAILED in awaited_states:
            mark_job_as_failed(
                job,
                "Not starting as dependency failed",
                code=StatusCode.DEPENDENCY_FAILED,
            )
            return

        if any(state != State.SUCCEEDED for state in awaited_states):
            set_message(
                job,
                "Waiting on dependencies",
                code=StatusCode.WAITING_ON_DEPENDENCIES,
            )
            return

        expected_state = ExecutorState.PREPARING
        new_status = api.prepare(definition)

    elif initial_status.state == ExecutorState.PREPARED:
        expected_state = ExecutorState.EXECUTING
        new_status = api.execute(definition)

    elif initial_status.state == ExecutorState.EXECUTED:
        expected_state = ExecutorState.FINALIZING
        new_status = api.finalize(definition)

    elif initial_status.state == ExecutorState.FINALIZED:
        # final state - we have finished!
        results = api.get_results(definition)
        save_results(job, results)
        obsolete = get_obsolete_files(definition, results.outputs)
        if obsolete:
            errors = api.delete_files(definition.workspace, Privacy.HIGH, obsolete)
            if errors:
                log.error(
                    f"Failed to delete high privacy files from workspace {definition.workspace}: {errors}"
                )
            api.delete_files(definition.workspace, Privacy.MEDIUM, obsolete)
            if errors:
                log.error(
                    f"Failed to delete medium privacy files from workspace {definition.workspace}: {errors}"
                )
        mark_job_as_completed(job)
        api.cleanup(definition)
        # we are done here
        return

    # following logic is common to all non-final transitions

    if new_status.state == initial_status.state:
        # no change in state, i.e. back pressure
        set_message(
            job,
            "Waiting on available resources",
            code=StatusCode.WAITING_ON_WORKERS,
        )

    elif new_status.state == expected_state:
        # successful state change to the expected next state
        if new_status.state == ExecutorState.PREPARING:
            job.state = State.RUNNING
            job.started_at = int(time.time())
        elif job.state != State.RUNNING:
            # got an ExecutorState that should mean the job.state is RUNNING, but it is not
            log.warning(
                f"state error: got {new_status.state} for job we thought was {job.state}"
            )
        set_message(job, new_status.state.value.title())

    elif new_status.state == ExecutorState.ERROR:
        # all transitions can go straight to error
        mark_job_as_failed(job, new_status.message)
        api.cleanup(definition)

    else:
        raise InvalidTransition(
            f"unexpected state transition of job {job.id} from {initial_status.state} to {new_status.state}: {new_status.message}"
        )


def save_results(job, results):
    """Extract the results of the execution and update the job accordingly."""
    # set the final state of the job
    if results.exit_code != 0:
        job.state = State.FAILED
        job.status_message = f"Job exited with error code {results.exit_code}"
        job.status_code = StatusCode.NONZERO_EXIT
        if results.message:
            job.status_message += f": {results.message}"
    elif results.unmatched_patterns:
        job.state = State.FAILED
        job.status_message = "No outputs found matching patterns:\n - {}".format(
            "\n - ".join(results.unmatched_patterns)
        )
        # If the job fails because an output was missing its very useful to
        # show the user what files were created as often the issue is just a
        # typo

        # Can we figure these out from job.outputs and project.yaml? Do we do
        # it here or just in local run?
        # TODO:  job.unmatched_outputs = ???
    else:
        job.state = State.SUCCEEDED
        job.status_message = "Completed successfully"

    job.outputs = results.outputs
    job.updated_at = int(time.time())
    update(job)


def get_obsolete_files(definition, outputs):
    """Get files that need to be deleted.

    These are files that we previously output by this action but were not
    output by the latest execution of it, so they've been removed or renamed.

    It does case insenstive comparison, as we don't know the the filesystems
    these will end up being stored on.
    """
    keep_files = {str(name).lower() for name in outputs}
    obsolete = []

    for existing in list_outputs_from_action(definition.workspace, definition.action):
        name = str(existing).lower()
        if name not in keep_files:
            obsolete.append(str(existing))
    return obsolete


def job_to_job_definition(job):

    action_args = shlex.split(job.run_command)
    allow_database_access = False
    env = {"OPENSAFELY_BACKEND": config.BACKEND}
    # Check `is True` so we fail closed if we ever get anything else
    if requires_db_access(action_args) is True:
        if not config.USING_DUMMY_DATA_BACKEND:
            allow_database_access = True
            env["DATABASE_URL"] = config.DATABASE_URLS[job.database_name]
            if config.TEMP_DATABASE_NAME:
                env["TEMP_DATABASE_NAME"] = config.TEMP_DATABASE_NAME
            if config.PRESTO_TLS_KEY and config.PRESTO_TLS_CERT:
                env["PRESTO_TLS_CERT"] = config.PRESTO_TLS_CERT
                env["PRESTO_TLS_KEY"] = config.PRESTO_TLS_KEY
            if config.EMIS_ORGANISATION_HASH:
                env["EMIS_ORGANISATION_HASH"] = config.EMIS_ORGANISATION_HASH
    # Prepend registry name
    image = action_args.pop(0)
    full_image = f"{config.DOCKER_REGISTRY}/{image}"
    if image.startswith("stata-mp"):
        env["STATA_LICENSE"] = str(config.STATA_LICENSE)

    # Jobs which are running reusable actions pull their code from the reusable
    # action repo, all other jobs pull their code from the study repo
    study = Study(job.action_repo_url or job.repo_url, job.action_commit or job.commit)
    # Both of action commit and repo_url should be set if either are
    assert bool(job.action_commit) == bool(job.action_repo_url)

    input_files = []
    for action in job.requires_outputs_from:
        for filename in list_outputs_from_action(job.workspace, action):
            input_files.append(filename)

    outputs = {}
    for privacy_level, named_patterns in job.output_spec.items():
        for name, pattern in named_patterns.items():
            outputs[pattern] = privacy_level

    return JobDefinition(
        id=job.id,
        job_request_id=job.job_request_id,
        study=study,
        workspace=job.workspace,
        action=job.action,
        created_at=job.created_at,
        image=full_image,
        args=action_args,
        env=env,
        inputs=input_files,
        output_spec=outputs,
        allow_database_access=allow_database_access,
        # in future, these may come from the JobRequest, but for now, we have
        # config defaults.
        cpu_count=config.DEFAULT_JOB_CPU_COUNT,
        memory_limit=config.DEFAULT_JOB_MEMORY_LIMIT,
    )


def get_states_of_awaited_jobs(job):
    job_ids = job.wait_for_job_ids
    if not job_ids:
        return []

    log.debug("Querying database for state of dependencies")
    states = select_values(Job, "state", id__in=job_ids)
    log.debug("Done query")
    return states


def mark_job_as_failed(job, error, code=None):
    if isinstance(error, str):
        message = error
    else:
        message = f"{type(error).__name__}: {error}"
    if job.cancelled:
        message = "Cancelled by user"
        code = StatusCode.CANCELLED_BY_USER
    set_state(job, State.FAILED, message, code=code)


def mark_job_as_running(job, message="Running"):
    set_state(job, State.RUNNING, message)


def mark_job_as_completed(job):
    # Completed means either SUCCEEDED or FAILED. We just save the job to the
    # database exactly as is with the exception of setting the completed at
    # timestamp
    assert job.state in [State.SUCCEEDED, State.FAILED]
    if job.state == State.FAILED and job.cancelled:
        job.status_message = "Cancelled by user"
        job.status_code = StatusCode.CANCELLED_BY_USER
    job.completed_at = int(time.time())
    log.debug("Updating full job record")
    update_job(job)
    log.debug("Update done")
    log.info(job.status_message, extra={"status_code": job.status_code})


def set_state(job, state, message, code=None):
    timestamp = int(time.time())
    if state == State.RUNNING:
        job.started_at = timestamp
    elif state == State.FAILED or state == State.SUCCEEDED:
        job.completed_at = timestamp
    elif state == State.PENDING:  # restarting the job gracefully
        job.started_at = None
    job.state = state
    job.status_message = message
    job.status_code = code
    job.updated_at = timestamp
    log.debug("Updating job status and timestamps")
    update_job(job)
    log.debug("Update done")
    log.info(job.status_message, extra={"status_code": job.status_code})


def set_message(job, message, code=None):
    timestamp = int(time.time())
    # If message has changed then update and log
    if job.status_message != message:
        job.status_message = message
        job.status_code = code
        job.updated_at = timestamp
        update_job(job)
        log.info(job.status_message, extra={"status_code": job.status_code})
    # If the status message hasn't changed then we only update the timestamp
    # once a minute. This gives the user some confidence that the job is still
    # active without writing to the database every single time we poll
    elif timestamp - job.updated_at >= 60:
        job.updated_at = timestamp
        log.debug("Updating job timestamp")
        update_job(job)
        log.debug("Update done")
        # For long running jobs we don't want to fill the logs up with "Job X
        # is still running" messages, but it is useful to have semi-regular
        # confirmations in the logs that it is still running. The below will
        # log approximately once every 10 minutes.
        if datetime.datetime.fromtimestamp(timestamp).minute % 10 == 0:
            log.info(job.status_message, extra={"status_code": job.status_code})


def get_reason_job_not_started(job):
    log.debug("Querying for running jobs")
    running_jobs = find_where(Job, state=State.RUNNING)
    log.debug("Query done")
    used_resources = sum(
        get_job_resource_weight(running_job) for running_job in running_jobs
    )
    required_resources = get_job_resource_weight(job)
    if used_resources + required_resources > config.MAX_WORKERS:
        if required_resources > 1:
            return "Waiting on available workers for resource intensive job"
        else:
            return "Waiting on available workers"


def list_outputs_from_action(workspace, action):
    for job in calculate_workspace_state(workspace):
        if job.action == action:
            return job.output_files

    # The action has never been run before
    return []


def get_job_resource_weight(job, weights=config.JOB_RESOURCE_WEIGHTS):
    """
    Get the job's resource weight by checking its workspace and action against
    the config file, default to 1 otherwise
    """
    action_patterns = weights.get(job.workspace)
    if action_patterns:
        for pattern, weight in action_patterns.items():
            if pattern.fullmatch(job.action):
                return weight
    return 1


def update_job(job):
    # The cancelled field is written by the sync thread and we should never update it. The sync thread never updates
    # any other fields after it has created the job, so we're always safe to modify them.
    update(job, exclude_fields=["cancelled"])


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
