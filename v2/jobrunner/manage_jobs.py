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
    allow_network_access = False
    env = {}
    if action_args[0].startswith("cohortextractor:"):
        if config.BACKEND == "expectations":
            action_args.extend(["--expectations-population", "10000"])
        else:
            allow_network_access = True
            env["DATABASE_URL"] = "foobar"
    # Prepend registry name
    action_args[0] = f"{config.DOCKER_REGISTRY}/{action_args[0]}"
    docker.run(
        container_name(job),
        action_args,
        volume=(volume, "/workspace"),
        env=env,
        allow_network_access=allow_network_access,
    )


def create_and_populate_volume(job):
    volume = volume_name(job)
    docker.create_volume(volume)
    # git-archive will create a tarball on stdout and docker cp will accept a
    # tarball on stdin, so if we wanted to we could do this all without a
    # temporary directory, but not worth it at this stage
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
    if not output_dir.exists():
        tmp_output_dir = output_dir.with_suffix(".tmp")
        error = save_job_outputs(job, tmp_output_dir)
        tmp_output_dir.rename(output_dir)
    docker.delete_container(container)
    docker.delete_volume(volume)
    if error:
        raise error


def save_job_outputs(job, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    container = container_name(job)
    volume = volume_name(job)
    # Dump container metadata
    container_metadata = docker.container_inspect(container, none_if_not_exists=True)
    if not container_metadata:
        return JobError("Job container has vanished")
    redact_environment_variables(container_metadata)
    with open(output_dir / "docker_metadata.json", "w") as f:
        json.dump(container_metadata, f, indent=2)
    # Dump job metadata
    job_metadata = job.asdict()
    job_request = find_where(SavedJobRequest, id=job.job_request_id)[0]
    job_metadata["job_request"] = job_request.original
    with open(output_dir / "job_metadata.json", "w") as f:
        json.dump(job_metadata, f, indent=2)
    # Dump Docker logs
    docker.write_logs_to_file(container, output_dir / "log.txt")
    # Extract specified outputs
    output_spec = flatten_file_spec(job.output_spec)
    patterns = output_spec.values()
    all_matches = docker.glob_volume_files(volume, patterns)
    unmatched_patterns = []
    for (privacy_level, name), pattern in output_spec.items():
        files = all_matches[pattern]
        if not files:
            unmatched_patterns.append(pattern)
        for filename in files:
            dest_filename = output_dir / name / filename
            dest_filename.parent.mkdir(parents=True, exist_ok=True)
            docker.copy_from_volume(volume, filename, dest_filename)
    # Return errors if appropriate
    if container_metadata["State"]["ExitCode"] != 0:
        return JobError("Job exited with an error code")
    if unmatched_patterns:
        unmatched_pattern_str = ", ".join(f"'{p}'" for p in unmatched_patterns)
        return JobError(f"No outputs found matching {unmatched_pattern_str}")


def flatten_file_spec(file_spec):
    flattened = {}
    for privacy_level, patterns in file_spec.items():
        for name, pattern in patterns.items():
            flattened[privacy_level, name] = pattern
    return flattened


# Environment variables whose values do not need to be hidden from the debug
# logs. At present the only sensitive value is DATABASE_URL, but its better to
# have an explicit safelist here.
SAFE_ENVIRONMENT_VARIABLES = set(
    """
    PATH PYTHON_VERSION DEBIAN_FRONTEND DEBCONF_NONINTERACTIVE_SEEN UBUNTU_VERSION
    PYENV_SHELL PYENV_VERSION PYTHONUNBUFFERED
    """.split()
)


def redact_environment_variables(container_metadata):
    env_vars = [line.split("=", 1) for line in container_metadata["Config"]["Env"]]
    redacted_vars = [
        f"{key}=xxxx-REDACTED-xxxx"
        if key not in SAFE_ENVIRONMENT_VARIABLES
        else f"{key}={value}"
        for (key, value) in env_vars
    ]
    container_metadata["Config"]["Env"] = redacted_vars


def job_still_running(job):
    return docker.container_is_running(container_name(job))


def container_name(job):
    return f"job-{job.id}"


def volume_name(job):
    return f"volume-{job.id}"


def outputs_exist(workspace, action):
    # TODO
    return False
