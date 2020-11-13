"""
This module contains the logic for starting jobs in Docker containers and
dealing with them when they are finished.

It's important that the `start_job` and `finalise_job` functions are
idempotent. This means that the job-runner can be killed at any point and will
still end up in a consistent state when it's restarted.
"""
import datetime
import json
import logging
import shlex
import shutil
import tempfile
import time

from . import config
from . import docker
from .database import find_where
from .git import checkout_commit
from .models import SavedJobRequest
from .project import is_generate_cohort_command


log = logging.getLogger(__name__)

# We use a file with this name to mark output directories as containing the
# results of successful runs. For debugging purposes we want to store the
# results even of failed runs, but we don't want to ever use them as the inputs
# to subsequent actions
SUCCESS_MARKER_FILE = ".success"


class JobError(Exception):
    pass


def start_job(job):
    # If we already created the job but were killed before we updated the state
    # then there's nothing further to do
    if docker.container_exists(container_name(job)):
        log.info("Container already created, nothing to do")
        return
    volume = create_and_populate_volume(job)
    action_args = shlex.split(job.run_command)
    allow_network_access = False
    env = {}
    if is_generate_cohort_command(action_args):
        if not config.USING_DUMMY_DATA_BACKEND:
            allow_network_access = True
            env["DATABASE_URL"] = config.DATABASE_URLS[job.database_name]
            if config.TEMP_DATABASE_NAME:
                env["TEMP_DATABASE_NAME"] = config.TEMP_DATABASE_NAME
    # Prepend registry name
    image = action_args[0]
    full_image = f"{config.DOCKER_REGISTRY}/{image}"
    # Newer versions of docker-cli support `--pull=never` as an argument to
    # `docker run` which would make this simpler, but it looks like it will be
    # a while before this makes it to Docker for Windows:
    # https://github.com/docker/cli/pull/1498
    if not docker.image_exists_locally(full_image):
        log.info(f"Image {full_image} not found locally (might need to docker pull)")
        raise JobError(f"Docker image {image} is not currently available")
    docker.run(
        container_name(job),
        [full_image] + action_args[1:],
        volume=(volume, "/workspace"),
        env=env,
        allow_network_access=allow_network_access,
    )


def create_and_populate_volume(job):
    volume = volume_name(job)
    docker.create_volume(volume)
    log.info(f"Copying in code from {job.repo_url}@{job.commit}")
    # git-archive will create a tarball on stdout and docker cp will accept a
    # tarball on stdin, so if we wanted to we could do this all without a
    # temporary directory, but not worth it at this stage
    config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=config.TMP_DIR) as tmpdir:
        checkout_commit(job.repo_url, job.commit, tmpdir)
        docker.copy_to_volume(volume, tmpdir)
    # Copy in files from dependencies
    for action in job.requires_outputs_from:
        action_dir = high_privacy_output_dir(job, action=action)
        log.info(f"Copying in outputs from {action_dir}")
        if not action_dir.joinpath(SUCCESS_MARKER_FILE).exists():
            raise JobError("Unexpected missing output for '{action}'")
        docker.copy_to_volume(volume, action_dir / "outputs")
    return volume


def finalise_job(job):
    """
    This involves checking whether the job finished successfully or not and
    extracting all outputs, logs and metadata
    """
    output_dir = high_privacy_output_dir(job)
    tmp_output_dir = output_dir.with_suffix(f".{job.id}.tmp")
    error = None
    try:
        save_job_outputs_and_metadata(job, tmp_output_dir)
    except JobError as e:
        error = e
    save_internal_metadata(job, tmp_output_dir, error)
    copy_logs_and_metadata_to_log_dir(job, tmp_output_dir)
    copy_medium_privacy_data(job, tmp_output_dir)
    if output_dir.exists():
        log.info("Deleting existing output directory")
        shutil.rmtree(output_dir)
    log.info(f"Renaming temporary directory to {output_dir}")
    tmp_output_dir.rename(output_dir)
    if error:
        raise error


def cleanup_job(job):
    log.info("Deleting container and volume")
    docker.delete_container(container_name(job))
    docker.delete_volume(volume_name(job))


def save_job_outputs_and_metadata(job, output_dir):
    """
    Saves all matching output files, container logs and container metadata to
    the output directory. Raises JobError *after* doing all this if there were
    any issues found.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    container = container_name(job)
    volume = volume_name(job)
    # Dump container metadata
    log.info("Dumping container metadata")
    container_metadata = docker.container_inspect(container, none_if_not_exists=True)
    if not container_metadata:
        raise JobError("Job container has vanished")
    redact_environment_variables(container_metadata)
    with open(output_dir / "docker_metadata.json", "w") as f:
        json.dump(container_metadata, f, indent=2)
    # Dump Docker logs
    log.info("Writing out Docker logs")
    docker.write_logs_to_file(container, output_dir / "logs.txt")
    # Extract specified outputs
    patterns = get_glob_patterns_from_spec(job.output_spec)
    all_matches = docker.glob_volume_files(volume, patterns)
    unmatched_patterns = []
    output_manifest = {}
    for pattern in patterns:
        files = all_matches[pattern]
        if not files:
            unmatched_patterns.append(pattern)
        for filename in files:
            dest_filename = output_dir / "outputs" / filename
            dest_filename.parent.mkdir(parents=True, exist_ok=True)
            # Only copy filles we haven't copied already: this means that if we
            # get interrupted while copying out several large files we don't
            # need to start again from scratch when we resume
            tmp_filename = dest_filename.with_suffix(".partial.tmp")
            if not dest_filename.exists():
                log.info(f"Copying {volume}:{filename} to {dest_filename}")
                docker.copy_from_volume(volume, filename, tmp_filename)
                tmp_filename.rename(dest_filename)
            output_manifest[filename] = {"size": dest_filename.stat().st_size}
    # Dump a record of all output files and their sizes
    log.info(f"Writing output manifest for {len(output_manifest)} files")
    with open(output_dir / "output_manifest.json", "w") as f:
        json.dump(output_manifest, f, indent=2)
    # Raise errors if appropriate
    if container_metadata["State"]["ExitCode"] != 0:
        raise JobError("Job exited with an error code")
    if unmatched_patterns:
        unmatched_pattern_str = ", ".join(f"'{p}'" for p in unmatched_patterns)
        raise JobError(f"No outputs found matching {unmatched_pattern_str}")


def save_internal_metadata(job, output_dir, error):
    """
    Saves a blob of JSON which includes the internal state of the job, and also
    the associated JobRequest (exactly as it was received from the job-server).
    This means that if the job-server includes data on e.g which user kicked
    off the job then this will be preserved on disk along with the job outputs.
    """
    log.info("Saving internal job metadata")
    job_metadata = job.asdict()
    job_request = find_where(SavedJobRequest, id=job.job_request_id)[0]
    job_metadata["job_request"] = job_request.original
    # There's a slight structural infelicity here: what we really want on disk
    # is the final state of the job. But the job won't transition into its
    # final state until after we've finished writing all the outputs. So
    # there's a little bit of duplication of the logic in `jobrunner.run` here
    # to anticpate what the final state will be.
    if error:
        job_metadata["status"] = "FAILED"
        job_metadata["status_message"] = f"{type(error).__name__}: {error}"
    else:
        job_metadata["status"] = "COMPLETED"
        job_metadata["status_message"] = "Completed successfully"
        # Create a marker file which we can use to easily determine if this
        # directory contains the outputs of a successful job which we can then
        # use elsewhere
        output_dir.joinpath(SUCCESS_MARKER_FILE).touch()
    job_metadata["completed_at"] = int(time.time())
    with open(output_dir / "job_metadata.json", "w") as f:
        json.dump(job_metadata, f, indent=2)


# Environment variables whose values do not need to be hidden from the debug
# logs. At present the only sensitive value is DATABASE_URL, but its better to
# have an explicit safelist here. We might end up including things like license
# keys in the environment.
SAFE_ENVIRONMENT_VARIABLES = set(
    """
    PATH PYTHON_VERSION DEBIAN_FRONTEND DEBCONF_NONINTERACTIVE_SEEN UBUNTU_VERSION
    PYENV_SHELL PYENV_VERSION PYTHONUNBUFFERED
    """.split()
)


def redact_environment_variables(container_metadata):
    """
    Redact the values of any environment variables in the container which
    aren't on the explicit safelist
    """
    env_vars = [line.split("=", 1) for line in container_metadata["Config"]["Env"]]
    redacted_vars = [
        f"{key}=xxxx-REDACTED-xxxx"
        if key not in SAFE_ENVIRONMENT_VARIABLES
        else f"{key}={value}"
        for (key, value) in env_vars
    ]
    container_metadata["Config"]["Env"] = redacted_vars


def copy_logs_and_metadata_to_log_dir(job, data_dir):
    """
    Ideally we'd keep all the output, logs, and metadata for every job and the
    workspace would just contain symlinks to the current version. Unfortunately
    Windows make this impractical. However the logs and metadata are small
    enough that we can copy these into separate log directories that we do keep
    around longer term.
    """
    # Split log directory up by month to make things slightly more manageable
    month_dir = datetime.date.today().strftime("%Y-%m")
    log_dir = config.JOB_LOG_DIR / month_dir / container_name(job)
    log.info(f"Copying logs and metadata to {log_dir}")
    log_dir.mkdir(parents=True, exist_ok=True)
    for filename in (
        "docker_metadata.json",
        "job_metadata.json",
        "output_manifest.json",
        "logs.txt",
    ):
        copy_file(data_dir / filename, log_dir / filename)


def copy_medium_privacy_data(job, source_dir):
    """
    Copies (rather than moves) all outputs specified as medium privacy to the
    medium privacy workspace. This does mean duplicate copies of the data but
    there's a big advantage in terms of operational simplicity and user
    experience to having the high privacy workspace contain all the outputs (of
    all privacy levels) in one place. It's also reasonable to assume that, by
    their nature, medium privacy outputs will be smaller than high privacy ones
    as they shouldn't contain large amounts of patient data.

    Along with the output files we also copy (some of) the metadata and logs to
    aid with debugging.
    """
    output_dir = medium_privacy_output_dir(job)
    dest_dir = output_dir.with_suffix(f".{job.id}.tmp")
    # We're copying most of the metadata here, the exception currently being
    # the Docker metadata which is probably of limited use to L4 users. We are
    # including a manifest giving the names and sizes of all output files
    # (including the high privacy ones) although obviously only the medium
    # privacy files have their contents copied as well. These seems like a
    # reasonably balance between helping debugging and maintaining L3 privacy.
    files_to_copy = {
        source_dir / "job_metadata.json",
        source_dir / "logs.txt",
        source_dir / "output_manifest.json",
    }
    patterns = get_glob_patterns_from_spec(job.output_spec, "moderately_sensitive")
    for pattern in patterns:
        files_to_copy.update(source_dir.joinpath("outputs").glob(pattern))
    for source_file in files_to_copy:
        if source_file.is_dir():
            continue
        relative_path = source_file.relative_to(source_dir)
        dest_file = dest_dir / relative_path
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        copy_file(source_file, dest_file)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    dest_dir.rename(output_dir)


def copy_file(source, dest):
    # shutil.copy() should be reasonably efficient in Python 3.8+, but if we
    # need to stick with 3.7 for some reason we could replace this with a
    # shellout to `cp`. See:
    # https://docs.python.org/3/library/shutil.html#shutil-platform-dependent-efficient-copy-operations
    shutil.copy(source, dest)


def get_glob_patterns_from_spec(output_spec, privacy_level=None):
    """
    Return all glob patterns from a file output specification matching the
    required privacy level, if supplied
    """
    assert privacy_level in [None, "highly_sensitive", "moderately_sensitive"]
    if privacy_level is None:
        # Return all patterns across all privacy levels
        return set().union(*[i.values() for i in output_spec.values()])
    else:
        return output_spec.get(privacy_level, {}).values()


def job_still_running(job):
    return docker.container_is_running(container_name(job))


# We use the slug (which is the ID with some human-readable stuff prepended)
# rather than just the opaque ID to make for easier debugging
def container_name(job):
    return f"job-{job.slug}"


def volume_name(job):
    return f"volume-{job.slug}"


# Note: this function can accept a JobRequest in place of a Job (anything with
# a workspace attribute will do)
def high_privacy_output_dir(job, action=None):
    workspace_dir = config.HIGH_PRIVACY_WORKSPACES_DIR / job.workspace
    if action is None:
        action = job.action
    return workspace_dir / action


def medium_privacy_output_dir(job, action=None):
    workspace_dir = config.MEDIUM_PRIVACY_WORKSPACES_DIR / job.workspace
    if action is None:
        action = job.action
    return workspace_dir / action


def outputs_exist(job_request, action):
    output_dir = high_privacy_output_dir(job_request, action)
    return output_dir.joinpath(SUCCESS_MARKER_FILE).exists()
