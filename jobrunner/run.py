"""
Script which polls the database for active (i.e. non-terminated) jobs, takes
the appropriate action for each job depending on its current state, and then
updates its state as appropriate.
"""

import collections
import datetime
import logging
import os
import sys
import time

from opentelemetry import trace

from jobrunner import config, tracing
from jobrunner.executors import get_executor_api
from jobrunner.job_executor import (
    ExecutorAPI,
    ExecutorRetry,
    ExecutorState,
    JobDefinition,
    Privacy,
    Study,
)
from jobrunner.lib import ns_timestamp_to_datetime
from jobrunner.lib.database import find_where, select_values, update
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.models import Job, State, StatusCode
from jobrunner.queries import calculate_workspace_state, get_flag_value


log = logging.getLogger(__name__)
tracer = trace.get_tracer("loop")

EXECUTOR_RETRIES = {}


class RetriesExceeded(Exception):
    pass


class InvalidTransition(Exception):
    pass


class ExecutorError(Exception):
    pass


def main(exit_callback=lambda _: False):  # pragma: no cover
    log.info("jobrunner.run loop started")
    api = get_executor_api()

    while True:
        with tracer.start_as_current_span("LOOP", attributes={"loop": True}):
            active_jobs = handle_jobs(api)

        if exit_callback(active_jobs):
            break

        time.sleep(config.JOB_LOOP_INTERVAL)


def handle_jobs(api: ExecutorAPI | None):
    log.debug("Querying database for active jobs")
    active_jobs = find_where(Job, state__in=[State.PENDING, State.RUNNING])
    log.debug("Done query")

    running_for_workspace = collections.defaultdict(int)
    handled_jobs = []

    while active_jobs:
        # We need to re-sort on each loop because the number of running jobs per
        # workspace will change as we work our way through
        active_jobs.sort(
            key=lambda job: (
                # Process all running jobs first. Once we've processed all of these, the
                # counts in `running_for_workspace` will be up-to-date.
                0 if job.state == State.RUNNING else 1,
                # Then process PENDING jobs in order of how many are running in the
                # workspace. This gives a fairer allocation of capacity among
                # workspaces.
                running_for_workspace[job.workspace],
                # DB jobs are more important than cpu jobs
                0 if job.requires_db else 1,
                # Finally use job age as a tie-breaker
                job.created_at,
            )
        )
        job = active_jobs.pop(0)

        # `set_log_context` ensures that all log messages triggered anywhere
        # further down the stack will have `job` set on them
        with set_log_context(job=job):
            handle_single_job(job, api)

        # Add running jobs to the workspace count
        if job.state == State.RUNNING:
            running_for_workspace[job.workspace] += 1

        handled_jobs.append(job)

    return handled_jobs


# we do not control the transition from these states, the executor does
STABLE_STATES = [
    ExecutorState.PREPARING,
    ExecutorState.EXECUTING,
    ExecutorState.FINALIZING,
]

# map ExecutorState to StatusCode
STATE_MAP = {
    ExecutorState.PREPARING: (
        StatusCode.PREPARING,
        "Preparing your code and workspace files",
    ),
    ExecutorState.PREPARED: (
        StatusCode.PREPARED,
        "Prepared and ready to run",
    ),
    ExecutorState.EXECUTING: (
        StatusCode.EXECUTING,
        "Executing job on the backend",
    ),
    ExecutorState.EXECUTED: (
        StatusCode.EXECUTED,
        "Job has finished executing and is waiting to be finalized",
    ),
    ExecutorState.FINALIZING: (
        StatusCode.FINALIZING,
        "Recording job results",
    ),
    ExecutorState.FINALIZED: (
        StatusCode.FINALIZED,
        "Finished recording results",
    ),
}


def handle_single_job(job, api):
    """The top level handler for a job.

    Mainly exists to wrap the job handling in an exception handler.
    """
    # we re-read the flags before considering each job, so make sure they apply
    # as soon as possible when set.
    mode = get_flag_value("mode")
    paused = str(get_flag_value("paused", "False")).lower() == "true"
    try:
        synchronous_transition = trace_handle_job(job, api, mode, paused)

        # provide a way to shortcut moving a job on to the next state right away
        # this is intended to support executors where some state transitions
        # are synchronous, particularly the local executor where prepare is
        # synchronous and can be time consuming.
        if synchronous_transition:
            trace_handle_job(job, api, mode, paused)
    except Exception as exc:
        mark_job_as_failed(
            job,
            StatusCode.INTERNAL_ERROR,
            "Internal error: this usually means a platform issue rather than a problem "
            "for users to fix.\n"
            "The tech team are automatically notified of these errors and will be "
            "investigating.",
            error=exc,
        )
        # Do not clean up, as we may want to debug
        #
        # Raising will kill the main loop, by design. The service manager
        # will restart, and this job will be ignored when it does, as
        # it has failed. If we have an internal error, a full restart
        # might recover better.
        raise


def trace_handle_job(job, api, mode, paused):
    """Call handle job with tracing."""
    attrs = {
        "initial_state": job.state.name,
        "initial_code": job.status_code.name,
    }

    with tracer.start_as_current_span("LOOP_JOB") as span:
        tracing.set_span_metadata(span, job, **attrs)
        try:
            synchronous_transition = handle_job(job, api, mode, paused)
        except Exception as exc:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
            span.record_exception(exc)
            raise
        else:
            span.set_attribute("final_state", job.state.name)
            span.set_attribute("final_code", job.status_code.name)

    return synchronous_transition


def handle_job(job, api, mode=None, paused=None):
    """Handle an active job.

    This contains the main state machine logic for a job. For the most part,
    state transitions follow the same logic, which is abstracted. Some
    transitions require special logic, mainly the initial and final states, as
    well as supporting cancellation and various operational modes.
    """
    assert job.state in (State.PENDING, State.RUNNING)
    job_definition = job_to_job_definition(job)

    # does this api have synchronous_transitions?
    synchronous_transitions = getattr(api, "synchronous_transitions", [])
    is_synchronous = False

    # only consider these modes if we are not about to cancel the job
    if not job_definition.cancelled:
        # handle special modes before considering executor state, as they ignore it
        if paused:
            if job.state == State.PENDING:
                # do not start the job, keep it pending
                set_code(
                    job,
                    StatusCode.WAITING_PAUSED,
                    "Backend is currently paused for maintenance, job will start once this is completed",
                )
                return

        if mode == "db-maintenance" and job_definition.allow_database_access:
            if job.state == State.RUNNING:
                log.warning(f"DB maintenance mode active, killing db job {job.id}")
                # we ignore the JobStatus returned from these API calls, as this is not a hard error
                api.terminate(job_definition)
                api.cleanup(job_definition)

            # reset state to pending and exit
            set_code(
                job,
                StatusCode.WAITING_DB_MAINTENANCE,
                "Waiting for database to finish maintenance",
            )
            return

    try:
        initial_status = api.get_status(job_definition)
    except ExecutorRetry as retry:
        job_retries = EXECUTOR_RETRIES.get(job.id, 0) + 1
        EXECUTOR_RETRIES[job.id] = job_retries
        span = trace.get_current_span()
        span.set_attribute("executor_retry", True)
        span.set_attribute("executor_retry_message", str(retry))
        span.set_attribute("executor_retry_count", job_retries)
        log.info(f"ExecutorRetry: {retry}")
        return
    else:
        EXECUTOR_RETRIES.pop(job.id, None)

    # cancelled is driven by user request, so is handled explicitly first
    if job_definition.cancelled:
        # if initial_status.state == ExecutorState.EXECUTED the job has already finished, so we
        # don't need to do anything here
        if initial_status.state == ExecutorState.EXECUTING:
            api.terminate(job_definition)  # synchronous operation
            new_status = api.get_status(job_definition)
            new_statuscode, _default_message = STATE_MAP[new_status.state]
            set_code(job, new_statuscode, "Cancelled whilst executing")
            return
        if initial_status.state == ExecutorState.PREPARED:
            set_code(
                job,
                StatusCode.FINALIZED,
                "Cancelled whilst prepared",
            )
            # Nb. no need to actually run finalize() in this case
            return
        if initial_status.state == ExecutorState.UNKNOWN:
            mark_job_as_failed(job, StatusCode.CANCELLED_BY_USER, "Cancelled by user")
            return

    # check if we've transitioned since we last checked and trace it.
    if initial_status.state in STATE_MAP:
        initial_code, initial_message = STATE_MAP[initial_status.state]
        if initial_code != job.status_code:
            set_code(
                job,
                initial_code,
                initial_message,
                timestamp_ns=initial_status.timestamp_ns,
            )

    # handle the simple no change needed states.
    if initial_status.state in STABLE_STATES:
        if job.state == State.PENDING:  # pragma: no cover
            log.warning(
                f"state error: got {initial_status.state} for a job we thought was PENDING"
            )
        # no action needed, simply update job message and timestamp, which is likely a no-op
        code, message = STATE_MAP[initial_status.state]
        set_code(job, code, message)
        return

    if initial_status.state == ExecutorState.ERROR:
        # something has gone wrong since we last checked
        raise ExecutorError(initial_status.message)

    # ok, handle the state transitions that are our responsibility
    if initial_status.state == ExecutorState.UNKNOWN:
        # a new job
        if job.state == State.RUNNING:  # pragma: no cover
            log.warning(
                "state error: got UNKNOWN state for a job we thought was RUNNING"
            )

        # check dependencies
        awaited_states = get_states_of_awaited_jobs(job)
        if State.FAILED in awaited_states:
            mark_job_as_failed(
                job,
                StatusCode.DEPENDENCY_FAILED,
                "Not starting as dependency failed",
            )
            return

        if any(state != State.SUCCEEDED for state in awaited_states):
            set_code(
                job,
                StatusCode.WAITING_ON_DEPENDENCIES,
                "Waiting on dependencies",
            )
            return

        # Temporary fix to reintroduce concurrency limits lost in the move to
        # the executor API. Ideally this should be the responsiblity of the
        # executor, but implementing that for the local executor requries some
        # work
        not_started_reason = get_reason_job_not_started(job)
        if not_started_reason:
            code, message = not_started_reason
            set_code(job, code, message)
            return

        if ExecutorState.PREPARING in synchronous_transitions:
            # prepare is synchronous, which means set our code to PREPARING
            # before calling  api.prepare(), and we expect it to be PREPARED
            # when finished
            code, message = STATE_MAP[ExecutorState.PREPARING]
            set_code(job, code, message)
            expected_state = ExecutorState.PREPARED
            is_synchronous = True
        else:
            expected_state = ExecutorState.PREPARING

        new_status = api.prepare(job_definition)

    elif initial_status.state == ExecutorState.PREPARED:
        expected_state = ExecutorState.EXECUTING
        new_status = api.execute(job_definition)

    elif initial_status.state == ExecutorState.EXECUTED:
        if ExecutorState.FINALIZING in synchronous_transitions:
            # finalize is synchronous, which means set our code to FINALIZING
            # before calling  api.finalize(), and we expect it to be FINALIZED
            # when finished
            code, message = STATE_MAP[ExecutorState.FINALIZING]
            set_code(job, code, message)
            expected_state = ExecutorState.FINALIZED
            is_synchronous = True
        else:
            expected_state = ExecutorState.FINALIZING

        new_status = api.finalize(job_definition)

    elif initial_status.state == ExecutorState.FINALIZED:  # pragma: no branch
        # Cancelled jobs that have had cleanup() should now be again set to cancelled here to ensure
        # they finish in the FAILED state
        if job_definition.cancelled:
            mark_job_as_failed(job, StatusCode.CANCELLED_BY_USER, "Cancelled by user")
            api.cleanup(job_definition)
            return

        # final state - we have finished!
        results = api.get_results(job_definition)

        save_results(job, job_definition, results)
        obsolete = get_obsolete_files(job_definition, results.outputs)

        # nb. obsolete is always empty due to a bug - see
        # https://github.com/opensafely-core/job-runner/issues/750
        if obsolete:  # pragma: no cover
            errors = api.delete_files(job_definition.workspace, Privacy.HIGH, obsolete)
            if errors:
                log.error(
                    f"Failed to delete high privacy files from workspace {job_definition.workspace}: {errors}"
                )
            api.delete_files(job_definition.workspace, Privacy.MEDIUM, obsolete)
            if errors:
                log.error(
                    f"Failed to delete medium privacy files from workspace {job_definition.workspace}: {errors}"
                )

        api.cleanup(job_definition)

        # we are done here
        return

    # following logic is common to all non-final transitions

    if new_status.state == initial_status.state:
        # no change in state, i.e. back pressure
        set_code(
            job,
            StatusCode.WAITING_ON_WORKERS,
            "Waiting on available resources",
        )

    elif new_status.state == expected_state:
        # successful state change to the expected next state
        code, message = STATE_MAP[new_status.state]
        set_code(job, code, message)

        # we want to immediately run this function for this job again to
        # avoid blocking it as we know the state transition has already
        # completed.
        return is_synchronous

    elif new_status.state == ExecutorState.ERROR:
        # all transitions can go straight to error
        raise ExecutorError(new_status.message)

    else:
        raise InvalidTransition(
            f"unexpected state transition of job {job.id} from {initial_status.state} to {new_status.state}: {new_status.message}"
        )


def save_results(job, job_definition, results):
    """Extract the results of the execution and update the job accordingly."""
    # save job outputs
    job.outputs = results.outputs
    job.level4_excluded_files = results.level4_excluded_files

    message = None
    error = False

    if results.exit_code != 0:
        code = StatusCode.NONZERO_EXIT
        error = True
        message = "Job exited with an error"
        if results.message:  # pragma: no cover
            message += f": {results.message}"
        elif job_definition.allow_database_access:
            error_msg = config.DATABASE_EXIT_CODES.get(results.exit_code)
            if error_msg:  # pragma: no cover
                message += f": {error_msg}"

    elif results.unmatched_patterns:
        job.unmatched_outputs = results.unmatched_outputs
        code = StatusCode.UNMATCHED_PATTERNS
        error = True
        # If the job fails because an output was missing its very useful to
        # show the user what files were created as often the issue is just a
        # typo
        message = "No outputs found matching patterns:\n - {}".format(
            "\n - ".join(results.unmatched_patterns)
        )

    else:
        code = StatusCode.SUCCEEDED
        message = "Completed successfully"

        if results.level4_excluded_files:
            message += f", but {len(results.level4_excluded_files)} file(s) marked as moderately_sensitive were excluded. See job log for details."

    set_code(job, code, message, error=error, results=results)


def get_obsolete_files(job_definition, outputs):
    """Get files that need to be deleted.

    These are files that we previously output by this action but were not
    output by the latest execution of it, so they've been removed or renamed.

    It does case insenstive comparison, as we don't know the the filesystems
    these will end up being stored on.
    """
    keep_files = {str(name).lower() for name in outputs}
    obsolete = []

    for existing in list_outputs_from_action(
        job_definition.workspace, job_definition.action
    ):
        name = str(existing).lower()
        if name not in keep_files:  # pragma: no cover
            obsolete.append(str(existing))
    return obsolete


def job_to_job_definition(job):
    allow_database_access = False
    env = {"OPENSAFELY_BACKEND": config.BACKEND}
    if job.requires_db:
        if not config.USING_DUMMY_DATA_BACKEND:
            allow_database_access = True
            env["DATABASE_URL"] = config.DATABASE_URLS[job.database_name]
            if config.TEMP_DATABASE_NAME:  # pragma: no cover
                env["TEMP_DATABASE_NAME"] = config.TEMP_DATABASE_NAME
            if config.PRESTO_TLS_KEY and config.PRESTO_TLS_CERT:  # pragma: no cover
                env["PRESTO_TLS_CERT"] = config.PRESTO_TLS_CERT
                env["PRESTO_TLS_KEY"] = config.PRESTO_TLS_KEY
            if config.EMIS_ORGANISATION_HASH:  # pragma: no cover
                env["EMIS_ORGANISATION_HASH"] = config.EMIS_ORGANISATION_HASH
    # Prepend registry name
    action_args = job.action_args
    image = action_args.pop(0)
    full_image = f"{config.DOCKER_REGISTRY}/{image}"
    if image.startswith("stata-mp"):  # pragma: no cover
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

    if job.cancelled:
        job_definition_cancelled = "user"
    else:
        job_definition_cancelled = None

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
        database_name=job.database_name if allow_database_access else None,
        # in future, these may come from the JobRequest, but for now, we have
        # config defaults.
        cpu_count=config.DEFAULT_JOB_CPU_COUNT,
        memory_limit=config.DEFAULT_JOB_MEMORY_LIMIT,
        level4_max_filesize=config.LEVEL4_MAX_FILESIZE,
        level4_max_csv_rows=config.LEVEL4_MAX_CSV_ROWS,
        level4_file_types=config.LEVEL4_FILE_TYPES,
        cancelled=job_definition_cancelled,
    )


def get_states_of_awaited_jobs(job):
    job_ids = job.wait_for_job_ids
    if not job_ids:
        return []

    log.debug("Querying database for state of dependencies")
    states = select_values(Job, "state", id__in=job_ids)
    log.debug("Done query")
    return states


def mark_job_as_failed(job, code, message, error=None, **attrs):
    if error is None:
        error = True

    set_code(job, code, message, error=error, **attrs)


def set_code(
    job, new_status_code, message, error=None, results=None, timestamp_ns=None, **attrs
):
    """Set the granular status code state.

    We also trace this transition with OpenTelemetry traces.

    Note: timestamp precision in the db is to the nearest second, which made
    sense when we were tracking fewer high level states. But now we are
    tracking more granular states, subsecond precision is needed to avoid odd
    collisions when states transition in <1s. Due to this, timestamp parameter
    should be the output of time.time() i.e. a float representing seconds.
    """
    if timestamp_ns is None:
        t = time.time()
        timestamp_s = int(t)
        timestamp_ns = int(t * 1e9)
    else:
        timestamp_s = int(timestamp_ns / 1e9)

    # if status code has changed then trace it and update
    if job.status_code != new_status_code:
        # handle timer measurement errors
        if job.status_code_updated_at > timestamp_ns:  # pragma: no cover
            # we somehow have a negative duration, which honeycomb does funny things with.
            # This can happen in tests, where things are fast, but we've seen it in production too.
            duration = datetime.timedelta(
                microseconds=int((timestamp_ns - job.status_code_updated_at) / 1e3)
            )
            log.warning(
                f"negative state duration of {duration}, clamping to 1ms\n"
                f"before: {job.status_code:<24} at {ns_timestamp_to_datetime(job.status_code_updated_at)}\n"
                f"after : {new_status_code:<24} at {ns_timestamp_to_datetime(timestamp_ns)}\n"
            )
            timestamp_ns = int(job.status_code_updated_at + 1e6)  # set duration to 1ms
            timestamp_s = int(timestamp_ns // 1e9)

        # update coarse state and timings for user
        if new_status_code in [StatusCode.PREPARED, StatusCode.PREPARING]:
            # we've started running
            job.state = State.RUNNING
            job.started_at = timestamp_s
        elif new_status_code in [StatusCode.CANCELLED_BY_USER]:
            # only set this cancelled status after any finalize/cleanup processes
            job.state = State.FAILED
        elif new_status_code.is_final_code:
            job.completed_at = timestamp_s
            if new_status_code == StatusCode.SUCCEEDED:
                job.state = State.SUCCEEDED
            else:
                job.state = State.FAILED
        # we sometimes reset the job back to pending
        elif new_status_code in [
            StatusCode.WAITING_ON_REBOOT,
            StatusCode.WAITING_DB_MAINTENANCE,
        ]:
            job.state = State.PENDING
            job.started_at = None

        # job trace: we finished the previous state
        tracing.finish_current_state(
            job, timestamp_ns, error=error, message=message, results=results, **attrs
        )

        # update db object
        job.status_code = new_status_code
        job.status_message = message
        job.updated_at = timestamp_s

        # use higher precision timestamp for state change time
        job.status_code_updated_at = timestamp_ns
        update_job(job)

        if new_status_code.is_final_code:
            # transitioning to a final state, so just record that state
            tracing.record_final_state(
                job,
                timestamp_ns,
                error=error,
                message=message,
                results=results,
                **attrs,
            )

        log.info(job.status_message, extra={"status_code": job.status_code})

    # If the status message hasn't changed then we only update the timestamp
    # once a minute. This gives the user some confidence that the job is still
    # active without writing to the database every single time we poll
    elif timestamp_s - job.updated_at >= 60:  # pragma: no cover
        job.updated_at = timestamp_s
        log.debug("Updating job timestamp")
        update_job(job)
        log.debug("Update done")
        # For long running jobs we don't want to fill the logs up with "Job X
        # is still running" messages, but it is useful to have semi-regular
        # confirmations in the logs that it is still running. The below will
        # log approximately once every 10 minutes.
        if datetime.datetime.fromtimestamp(timestamp_s).minute % 10 == 0:
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
        if required_resources > 1:  # pragma: no cover
            return (
                StatusCode.WAITING_ON_WORKERS,
                "Waiting on available workers for resource intensive job",
            )
        else:
            return StatusCode.WAITING_ON_WORKERS, "Waiting on available workers"

    if job.requires_db:
        running_db_jobs = len([j for j in running_jobs if j.requires_db])
        if running_db_jobs >= config.MAX_DB_WORKERS:
            return (
                StatusCode.WAITING_ON_DB_WORKERS,
                "Waiting on available database workers",
            )

    if os.environ.get("FUNTIMES", False):  # pragma: no cover
        # allow any db job to run
        if job.requires_db:
            return None

        # allow OSI non-db jobs to run
        if job.workspace.endswith("-interactive"):
            return None

        # nope all other jobs
        return StatusCode.WAITING_ON_WORKERS, "Waiting on available workers"


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


if __name__ == "__main__":  # pragma: no cover
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
