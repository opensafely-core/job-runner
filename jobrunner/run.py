"""
Script which polls the database for active (i.e. non-terminated) jobs, takes
the appropriate action for each job depending on its current state, and then
updates its state as appropriate.
"""
import datetime
import logging
import time

from .log_utils import configure_logging, set_log_context
from . import config
from .database import find_where, count_where, update, select_values
from .models import Job, State
from .manage_jobs import (
    JobError,
    start_job,
    job_still_running,
    finalise_job,
    cleanup_job,
)


log = logging.getLogger(__name__)


def main(exit_when_done=False):
    while True:
        active_job_count = handle_jobs()
        if exit_when_done and active_job_count == 0:
            break
        time.sleep(config.JOB_LOOP_INTERVAL)


def handle_jobs():
    active_jobs = find_where(Job, status__in=[State.PENDING, State.RUNNING])
    for job in active_jobs:
        # `set_log_context` ensures that all log messages triggered anywhere
        # further down the stack will have `job` set on them
        with set_log_context(job=job):
            if job.status == State.PENDING:
                handle_pending_job(job)
            elif job.status == State.RUNNING:
                handle_running_job(job)
    return len(active_jobs)


def handle_pending_job(job):
    awaited_states = get_states_of_awaited_jobs(job)
    if State.FAILED in awaited_states:
        mark_job_as_failed(job, JobError("Not starting as dependency failed"))
    elif any(state != State.COMPLETED for state in awaited_states):
        set_message(job, "Waiting on dependencies", timestamp=True)
    else:
        if not job_running_capacity_available():
            set_message(job, "Waiting for available workers", timestamp=True)
        else:
            try:
                set_message(job, "Starting")
                start_job(job)
            except JobError as e:
                mark_job_as_failed(job, e)
                cleanup_job(job)
            except Exception:
                mark_job_as_failed(job, JobError("Internal error when starting job"))
                raise
            else:
                mark_job_as_running(job)


def handle_running_job(job):
    if job_still_running(job):
        set_message(job, "Running", timestamp=True)
    else:
        try:
            set_message(job, "Finished, checking status and extracting outputs")
            finalise_job(job)
        except JobError as e:
            mark_job_as_failed(job, e)
            # Question: do we want to clean up failed jobs? Given that we now
            # tag all job-runner volumes and containers with a specific label
            # we could leave them around for debugging purposes and have a
            # cronjob which cleans them up a few days after they've stopped.
            cleanup_job(job)
        except Exception:
            mark_job_as_failed(job, JobError("Internal error when finalising job"))
            # We deliberately don't clean up after an internal error so we have
            # some change of debugging. It's also possible, after fixing the
            # error, to manually flip the state of the job back to "running" in
            # the database and the code will then be able to finalise it
            # correctly without having to re-run the job.
            raise
        else:
            mark_job_as_completed(job)
            cleanup_job(job)


def get_states_of_awaited_jobs(job):
    job_ids = job.wait_for_job_ids
    if not job_ids:
        return []
    return select_values(Job, "status", id__in=job_ids)


def mark_job_as_failed(job, exception):
    job.status = State.FAILED
    job.status_message = f"{type(exception).__name__}: {exception}"
    update(job, update_fields=["status", "status_message"])
    log.info(job.status_message)


def mark_job_as_running(job):
    job.status = State.RUNNING
    job.status_message = "Started"
    update(job, update_fields=["status", "status_message"])
    log.info(job.status_message)


def mark_job_as_completed(job):
    job.status = State.COMPLETED
    job.status_message = "Completed successfully"
    update(job, update_fields=["status", "status_message"])
    log.info(job.status_message)


def job_running_capacity_available():
    running_jobs = count_where(Job, status=State.RUNNING)
    return running_jobs < config.MAX_WORKERS


def set_message(job, message, timestamp=False):
    # A bit of a hack, but hopefully worthwhile: there are certain states
    # (waiting and running) which we can expect some jobs to stay in for quite
    # a long time. We don't want to spam the logs (or the database) by writing
    # these out everytime. But if we only write them when they change then we
    # may have to look a long way back in the logs to find the last update, and
    # it's harder for users to have confidence that the job really is still
    # running or waiting. By appending a timestamp and ignoring the final digit
    # when checking for changes we end up only logging the state every 10
    # minutes which seems about right.
    if timestamp:
        now = datetime.datetime.now(datetime.timezone.utc)
        message = f"{message} at {now:%Y-%m-%d %H:%M} UTC"
        if job.status_message:
            changed = job.status_message[:-5] != message[:-5]
        else:
            changed = True
    else:
        changed = job.status_message != message
    if changed:
        job.status_message = message
        update(job, update_fields=["status_message"])
        log.info(job.status_message)


if __name__ == "__main__":
    configure_logging()
    main()
