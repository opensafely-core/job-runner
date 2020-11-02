import time

from . import config
from .database import find_where, update, select_values
from .models import Job, State
from .manage_containers import (
    JobError,
    start_job_running,
    job_still_running,
    finalise_job,
)


def main(exit_when_done=False):
    while True:
        job_count = handle_jobs()
        if exit_when_done and job_count == 0:
            break
        time.sleep(config.JOB_LOOP_INTERVAL)


def handle_jobs():
    jobs = find_where(Job, status__in=[State.PENDING, State.RUNNING])
    for job in jobs:
        handle_job(job)
    return len(jobs)


def handle_job(job):
    if job.status == State.PENDING:
        awaited_states = get_states_of_awaited_jobs(job)
        if State.FAILED in awaited_states:
            mark_job_as_failed(job, JobError("Not starting as dependency failed"))
        elif all(state == State.COMPLETED for state in awaited_states):
            if job_running_capacity_available():
                try:
                    start_job_running(job)
                except JobError as e:
                    mark_job_as_failed(job, e)
                else:
                    mark_job_as_running(job)
    elif job.status == State.RUNNING:
        if not job_still_running(job):
            try:
                outputs = finalise_job(job)
            except JobError as e:
                mark_job_as_failed(job, e)
            else:
                mark_job_as_completed(job, outputs)


def get_states_of_awaited_jobs(job):
    job_ids = job.wait_for_job_ids
    if not job_ids:
        return []
    return select_values(Job, "status", id__in=job_ids)


def mark_job_as_failed(job, exception):
    job.status = State.FAILED
    job.error_message = f"{type(exception).__name__}: {exception}"
    update(job, update_fields=["status", "error_message"])


def mark_job_as_running(job):
    job.status = State.RUNNING
    update(job, update_fields=["status"])


def mark_job_as_completed(job, outputs):
    job.status = State.COMPLETED
    job.output_files = outputs
    update(job, update_fields=["status", "output_files"])


def job_running_capacity_available():
    # TODO: Decide what to do here
    return True


if __name__ == "__main__":
    main()
