"""
Script which polls the database for active (i.e. non-terminated) jobs, takes
the appropriate action for each job depending on its current state, and then
updates its state as appropriate.
"""
import datetime
import logging
import random
import sys
import time

from jobrunner import config
from jobrunner.lib.database import find_where, select_values, update
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.manage_jobs import (
    kill_job,
    start_job, sync_job_status,
)
from jobrunner.models import Job, State, StatusCode, JobError

log = logging.getLogger(__name__)


def main(exit_callback=lambda _: False):
    log.info("jobrunner.run loop started")
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
                handle_pending_job(job)
            elif job.state == State.RUNNING:
                handle_running_job(job)
    return active_jobs


def handle_pending_job(job):
    if job.cancelled:
        # Mark the job as running and then immediately invoke
        # `handle_running_job` to deal with the cancellation. This slightly
        # counterintuitive approach allows us to keep a simple, consistent set
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
            except Exception:
                mark_job_as_failed(job, "Internal error when starting job")
                raise
            else:
                mark_job_as_running(job)


def handle_running_job(job):
    if job.cancelled:
        log.info("Cancellation requested, killing job")
        kill_job(job)

    try:
        is_running = sync_job_status(job)

        if is_running:
            set_message(job, "Running")
        else:
            set_message(job, "Finished")
            mark_job_as_completed(job)
    except JobError as exception:
        set_message(job, "Failed")
        mark_job_as_failed(job, exception)
    except Exception:
        set_message(job, "Failed")
        mark_job_as_failed(job, "Internal error when finalising job")
        raise


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
