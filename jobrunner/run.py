"""
Script which polls the database for active (i.e. non-terminated) jobs, takes
the appropriate action for each job depending on its current state, and then
updates its state as appropriate.
"""
import datetime
import logging
import random
import sys
import shlex
import time
from pathlib import Path

from jobrunner import config
from jobrunner.lib.database import find_where, select_values, update
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.manage_jobs import (
    BrokenContainerError,
    JobError,
    cleanup_job,
    finalise_job,
    job_still_running,
    kill_job,
    start_job,
    get_job_metadata,
    read_manifest_file,
    write_manifest_file,
    update_manifest,
)
from jobrunner.models import Job, State, StatusCode
from jobrunner import job_executor
from jobrunner.project import (
    is_generate_cohort_command,
)


log = logging.getLogger(__name__)


def main(exit_callback=lambda _: False):
    log.info("jobrunner.run loop started")
    if config.EXECUTION_API:
        log.info("using new EXECUTION_API")
    while True:
        active_jobs = handle_jobs()
        if exit_callback(active_jobs):
            break
        time.sleep(config.JOB_LOOP_INTERVAL)


def handle_jobs():
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
            if job.state == State.PENDING:
                if config.EXECUTION_API:
                    handle_pending_job_api(job, job_executor.get_job_api())
                else:
                    handle_pending_job(job)
            elif job.state == State.RUNNING:
                if config.EXECUTION_API:
                    handle_running_job_api(job, job_executor.get_job_api())
                else:
                    handle_running_job(job)
    return active_jobs


def handle_pending_job(job):
    if job.cancelled:
        # Mark the job as running and then immediately invoke
        # `handle_running_job` to deal with the cancellation. This slightly
        # counterintuitive appraoch allows us to keep a simple, consistent set
        # of state transitions and to consolidate all the kill/cleanup code
        # together. It also means that there aren't edge cases where we could
        # lose track of jobs completely after losing database state
        mark_job_as_running(job)
        handle_running_job(job)
        return

    awaited_states = get_states_of_awaited_jobs(job)
    if State.FAILED in awaited_states:
        mark_job_as_failed(
            job, "Not starting as dependency failed", code=StatusCode.DEPENDENCY_FAILED
        )
    elif any(state != State.SUCCEEDED for state in awaited_states):
        set_message(
            job, "Waiting on dependencies", code=StatusCode.WAITING_ON_DEPENDENCIES
        )
    else:
        not_started_reason = get_reason_job_not_started(job)
        if not_started_reason:
            set_message(job, not_started_reason, code=StatusCode.WAITING_ON_WORKERS)
        else:
            try:
                set_message(job, "Preparing")
                start_job(job)
            except JobError as exception:
                mark_job_as_failed(job, exception)
                # See the `raise` in manage_jobs which explains why we can't
                # cleanup on this specific error
                if not isinstance(exception, BrokenContainerError):
                    cleanup_job(job)
            except Exception:
                mark_job_as_failed(job, "Internal error when starting job")
                cleanup_job(job)
                raise
            else:
                mark_job_as_running(job)


def handle_pending_job_api(job, api):
    if job.cancelled:
        # Mark the job as running and then immediately invoke
        # `handle_running_job` to deal with the cancellation. This slightly
        # counterintuitive appraoch allows us to keep a simple, consistent set
        # of state transitions and to consolidate all the kill/cleanup code
        # together. It also means that there aren't edge cases where we could
        # lose track of jobs completely after losing database state
        mark_job_as_running(job)
        handle_running_job_api(job, api)
        return

    awaited_states = get_states_of_awaited_jobs(job)
    if State.FAILED in awaited_states:
        mark_job_as_failed(
            job, "Not starting as dependency failed", code=StatusCode.DEPENDENCY_FAILED
        )
    elif any(state != State.SUCCEEDED for state in awaited_states):
        set_message(
            job, "Waiting on dependencies", code=StatusCode.WAITING_ON_DEPENDENCIES
        )
    else:
        not_started_reason = get_reason_job_not_started(job)
        if not_started_reason:
            set_message(job, not_started_reason, code=StatusCode.WAITING_ON_WORKERS)
        else:
            try:
                set_message(job, "Preparing")
                api.run(job_to_job_definition(job))
            except JobError as exception:
                mark_job_as_failed(job, exception)
                # support any clean up needed
                api.cleanup(job_to_job_definition(job))
            except Exception:
                mark_job_as_failed(job, "Internal error when starting job")
                raise
            else:
                mark_job_as_running(job)


def handle_running_job(job):
    if job.cancelled:
        log.info("Cancellation requested, killing job")
        kill_job(job)

    log.debug("Checking job running state")
    is_running = job_still_running(job)
    log.debug("Check done")
    if is_running:
        set_message(job, "Running")
    else:
        try:
            set_message(job, "Finished, checking status and extracting outputs")
            job = finalise_job(job)
            # We expect the job to be transitioned into its final state at this
            # point
            assert job.state in [State.SUCCEEDED, State.FAILED]
        except JobError as exception:
            mark_job_as_failed(job, exception)
            # Question: do we want to clean up failed jobs? Given that we now
            # tag all job-runner volumes and containers with a specific label
            # we could leave them around for debugging purposes and have a
            # cronjob which cleans them up a few days after they've stopped.
            cleanup_job(job)
        except Exception:
            mark_job_as_failed(job, "Internal error when finalising job")
            # We deliberately don't clean up after an internal error so we have
            # some change of debugging. It's also possible, after fixing the
            # error, to manually flip the state of the job back to "running" in
            # the database and the code will then be able to finalise it
            # correctly without having to re-run the job.
            raise
        else:
            mark_job_as_completed(job)
            cleanup_job(job)


def handle_running_job_api(job, api):
    if job.cancelled:
        log.info("Cancellation requested, killing job")
        api.terminate(job_to_job_definition(job))

    try:
        is_running = sync_job_status(job, api)

        if is_running:
            set_message(job, "Running")
        else:
            mark_job_as_completed(job)
            api.cleanup(job_to_job_definition(job))
    except JobError as exception:
        set_message(job, "Failed")
        mark_job_as_failed(job, exception)
        api.cleanup(job_to_job_definition(job))
    except Exception:
        set_message(job, "Failed")
        mark_job_as_failed(job, "Internal error when finalising job")
        # We don't clean up here, to facilitate with debugging this unexpected error
        raise


def job_to_job_definition(job):
    action_args = shlex.split(job.run_command)
    allow_database_access = False
    env = {"OPENSAFELY_BACKEND": config.BACKEND}
    # Check `is True` so we fail closed if we ever get anything else
    if is_generate_cohort_command(action_args) is True:
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
    study = job_executor.Study(
        job.action_repo_url or job.repo_url, job.action_commit or job.commit
    )
    # Both of action commit and repo_url should be set if either are
    assert bool(job.action_commit) == bool(job.action_repo_url)

    input_files = []
    for action in job.requires_outputs_from:
        for filename in list_outputs_from_action(action):
            input_files.append(filename)

    outputs = {}
    for privacy_level, named_patterns in job.output_spec.items():
        for name, pattern in named_patterns.items():
            outputs[pattern] = privacy_level

    return job_executor.JobDefinition(
        job.id,
        study,
        job.workspace,
        job.action,
        full_image,
        action_args,
        env,
        input_files,
        outputs,
        allow_database_access,
    )


def sync_job_status(job, api):
    """Query API for job status."""
    state, results = api.get_status(job_to_job_definition(job))

    if state == State.RUNNING:
        return True
    assert state != State.PENDING

    # TODO: implement workspace state tracking
    # delete_obsolete_files(job, results)

    job.state = state
    job.image_id = results.image_id
    job.outputs = results.outputs
    set_message(job, results.status_message, results.status_code)
    update(job)

    workspace_dir = Path(config.HIGH_PRIVACY_STORAGE_BASE, job.workspace)

    # fake a Docker container metadata just enough for now
    container_metadata = {
        "State": {"ExitCode": results.exit_code},
        "Image": results.image_id,
    }

    job_metadata = get_job_metadata(job, container_metadata)

    manifest = read_manifest_file(workspace_dir)
    update_manifest(manifest, job_metadata)
    write_manifest_file(workspace_dir, manifest)

    return False


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


def mark_job_as_running(job):
    set_state(job, State.RUNNING, "Running")


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
    update(job)
    log.debug("Update done")
    log.info(job.status_message, extra={"status_code": job.status_code})


def set_state(job, state, message, code=None):
    timestamp = int(time.time())
    if state == State.RUNNING:
        job.started_at = timestamp
    elif state == State.FAILED or state == State.SUCCEEDED:
        job.completed_at = timestamp
    job.state = state
    job.status_message = message
    job.status_code = code
    job.updated_at = timestamp
    log.debug("Updating job status and timestamps")
    update(
        job,
        update_fields=[
            "image_id",
            "state",
            "status_message",
            "status_code",
            "updated_at",
            "started_at",
            "completed_at",
        ],
    )
    log.debug("Update done")
    log.info(job.status_message, extra={"status_code": job.status_code})


def set_message(job, message, code=None):
    timestamp = int(time.time())
    # If message has changed then update and log
    if job.status_message != message:
        job.status_message = message
        job.status_code = code
        job.updated_at = timestamp
        update(job, update_fields=["status_message", "status_code", "updated_at"])
        log.info(job.status_message, extra={"status_code": job.status_code})
    # If the status message hasn't changed then we only update the timestamp
    # once a minute. This gives the user some confidence that the job is still
    # active without writing to the database every single time we poll
    elif timestamp - job.updated_at >= 60:
        job.updated_at = timestamp
        log.debug("Updating job timestamp")
        update(job, update_fields=["updated_at"])
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


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
