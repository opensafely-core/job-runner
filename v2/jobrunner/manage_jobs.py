"""
This module contains the logic for starting jobs in Docker containers and
dealing with them when they are finished.

It's important that the `start_job` and `finalise_job` functions are
idempotent. This means that the job-runner can be killed at any point and will
still end up in a consistent state when it's restarted.
"""
import json
import shlex
import tempfile

from . import config
from . import docker
from .database import find_where
from .git import checkout_commit
from .models import SavedJobRequest


class JobError(Exception):
    pass


def start_job(job):
    # If we started the job but were killed before we updated the state then
    # there's nothing further to do
    if job_still_running(job):
        return
    volume = create_and_populate_volume(job)
    action_args = shlex.split(job.run_command)
    # allow_networking = False
    env = {}
    if action_args[0].startswith("docker.opensafely.org/cohortextractor:"):
        if config.BACKEND == "expectations":
            action_args.extend(["--expectations-population", "10000"])
        else:
            # allow_networking = True
            env["DATABASE_URL"] = "foobar"
    docker.run(container_name(job), action_args, volume=(volume, "/workspace"), env=env)


def create_and_populate_volume(job):
    volume = volume_name(job)
    docker.create_volume(volume)
    with tempfile.TemporaryDirectory() as tmpdir:
        checkout_commit(job.repo_url, job.commit, tmpdir)
        docker.copy_to_volume(volume, tmpdir)
    # copy in files from dependencies
    for action in job.requires_outputs_from:
        pass
    return volume


def finalise_job(job):
    container = container_name(job)
    volume = volume_name(job)
    output_dir = config.WORK_DIR / "outputs" / container
    output_dir.mkdir(parents=True, exist_ok=True)
    container_metadata = docker.container_inspect(container, none_if_not_exists=True)
    # container_metadata = redact_env_vars(container_metadata)
    with open(output_dir / "docker_metadata.json", "w") as f:
        json.dump(container_metadata, f, indent=2)
    job_metadata = job.asdict()
    job_request = find_where(SavedJobRequest, id=job.job_request_id)[0]
    job_metadata["job_request"] = job_request.original
    with open(output_dir / "job_metadata.json", "w") as f:
        json.dump(job_metadata, f, indent=2)
    docker.write_logs_to_file(container, output_dir / "log.txt")
    # glob all matching outputs
    output_spec = flatten_file_spec(job.output_spec)
    patterns = output_spec.values()
    all_matches = docker.glob_volume_files(volume, patterns)
    for (privacy_level, name), pattern in output_spec.items():
        files = all_matches[pattern]
        for filename in files:
            dest_filename = output_dir / name / filename
            dest_filename.parent.mkdir(parents=True, exist_ok=True)
            docker.copy_from_volume(volume, filename, dest_filename)
    docker.delete_container(container)
    docker.delete_volume(volume)


def flatten_file_spec(file_spec):
    flattened = {}
    for privacy_level, patterns in file_spec.items():
        for name, pattern in patterns.items():
            flattened[privacy_level, name] = pattern
    return flattened


def job_still_running(job):
    return docker.container_is_running(container_name(job))


def container_name(job):
    return f"job-{job.id}"


def volume_name(job):
    return f"volume-{job.id}"


def outputs_exist(workspace, action):
    # TODO
    return False
