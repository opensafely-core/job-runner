import json
import shlex
import tempfile
import time


from . import config
from . import docker
from .database import find_where, update, select_values
from .git import checkout_commit
from .models import Job, State


class JobError(Exception):
    pass


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


def start_job_running(job):
    # If we started the job but were killed before we updated the state then
    # there's nothing further to do
    if job_still_running(job):
        return
    volume = volume_name(job)
    docker.create_volume(volume)
    with tempfile.TemporaryDirectory() as tmpdir:
        checkout_commit(job.repo_url, job.commit, tmpdir)
        docker.copy_to_volume(volume, tmpdir)
    # copy in files from dependencies
    # start container
    for action in job.requires_outputs_from:
        pass
    action_args = shlex.split(job.run_command)
    docker.run(container_name(job), action_args, volume=(volume, "/workspace"), env={})


def finalise_job(job):
    container = container_name(job)
    output_dir = config.WORK_DIR / "outputs" / container
    output_dir.mkdir(parents=True, exist_ok=True)
    container_metadata = docker.container_inspect(container, none_if_not_exists=True)
    with open(output_dir / "docker_metadata.json", "w") as f:
        json.dump(container_metadata, f, indent=2)
    docker.write_logs_to_file(container, output_dir / "docker_log.txt")
    docker.delete_container(container)
    docker.delete_volume(volume_name(job))


def get_states_of_awaited_jobs(job):
    job_ids = job.wait_for_job_ids
    if not job_ids:
        return []
    return select_values(Job, "status", id__in=job_ids)


def job_still_running(job):
    return docker.container_is_running(container_name(job))


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


def container_name(job):
    return f"job-{job.id}"


def volume_name(job):
    return f"volume-{job.id}"


if __name__ == "__main__":
    main()
