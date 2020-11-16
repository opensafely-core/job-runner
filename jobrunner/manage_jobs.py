"""
This module contains the logic for starting jobs in Docker containers and
dealing with them when they are finished.

It's important that the `start_job` and `finalise_job` functions are
idempotent. This means that the job-runner can be killed at any point and will
still end up in a consistent state when it's restarted.
"""
import copy
import datetime
import json
import logging
from pathlib import PurePosixPath, Path
import shlex
import shutil
import tempfile
import time

from . import config
from . import docker
from .database import find_where
from .git import checkout_commit
from .models import SavedJobRequest, State
from .project import is_generate_cohort_command


log = logging.getLogger(__name__)

# Directory inside working directory where manifest and logs are created
METADATA_DIR = "metadata"

# Records details of which action created each file (leading underscore so it
# gets listed before all the log files)
MANIFEST_FILE = "_manifest.json"


class JobError(Exception):
    pass


class MissingOutputError(JobError):
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
    input_files = []
    for action in job.requires_outputs_from:
        input_files.extend(list_outputs_from_action(job.workspace, action))
    volume = volume_name(job)
    docker.create_volume(volume)
    log.info(f"Copying in code from {job.repo_url}@{job.commit}")
    # git-archive will create a tarball on stdout and docker cp will accept a
    # tarball on stdin, so if we wanted to we could do this all without a
    # temporary directory, but not worth it at this stage
    config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=config.TMP_DIR) as tmpdir:
        tmpdir = Path(tmpdir)
        checkout_commit(job.repo_url, job.commit, tmpdir)
        # Because `docker cp` can't create parent directories automatically, we
        # make sure parent directories exist for all the files we're going to
        # copy in
        directories = set(PurePosixPath(filename).parent for filename in input_files)
        for directory in directories:
            tmpdir.joinpath(directory).mkdir(parents=True, exist_ok=True)
        docker.copy_to_volume(volume, tmpdir, ".")
    workspace_dir = config.HIGH_PRIVACY_WORKSPACES_DIR / job.workspace
    for filename in input_files:
        log.info(f"Copying in {filename}")
        docker.copy_to_volume(volume, workspace_dir / filename, filename)
    return volume


def job_still_running(job):
    return docker.container_is_running(container_name(job))


# We use the slug (which is the ID with some human-readable stuff prepended)
# rather than just the opaque ID to make for easier debugging
def container_name(job):
    return f"job-{job.slug}"


def volume_name(job):
    return f"volume-{job.slug}"


def finalise_job(job):
    """
    This involves checking whether the job finished successfully or not and
    extracting all outputs, logs and metadata
    """
    container_metadata = get_container_metadata(job)
    outputs, unmatched_patterns = find_matching_outputs(job)

    # Extract outputs to workspaces
    high_privacy_dir = config.HIGH_PRIVACY_WORKSPACES_DIR / job.workspace
    med_privacy_dir = config.MEDIUM_PRIVACY_WORKSPACES_DIR / job.workspace
    volume = volume_name(job)
    for filename, privacy_level in outputs.items():
        docker.copy_from_volume(volume, filename, high_privacy_dir / filename)
        if privacy_level == "moderately_sensitive":
            copy_file(high_privacy_dir / filename, med_privacy_dir / filename)

    # Error handling is slightly tricky here: for most classes of error we
    # still want to extract outputs and logs for debugging purposes, so we
    # can't just raise an exception at this point. But we need to know now if
    # there are any errors so we can write the appropriate state to disk.  So
    # we create the error here, pass it to `get_job_metadata` so it can persist
    # the correct state, run all the extraction code, and then raise the
    # exception at the end once we've updated everything.
    if container_metadata["State"]["ExitCode"] != 0:
        error = JobError("Job exited with an error code")
    elif unmatched_patterns:
        error = JobError(f"No outputs found matching: {', '.join(unmatched_patterns)}")
    else:
        error = None

    # job_metadata is a big dict capturing everything we know about the state
    # of the job
    job_metadata = get_job_metadata(job, container_metadata, outputs, error)

    # Dump useful info in log directory
    log_dir = get_log_dir(job)
    write_log_file(job, job_metadata, log_dir / "logs.txt")
    with open(log_dir / "metadata.json", "w") as f:
        json.dump(job_metadata, f, indent=2)

    # Copy logs to workspaces
    copy_file(
        log_dir / "logs.txt", high_privacy_dir / METADATA_DIR / f"{job.action}.log"
    )
    copy_file(
        log_dir / "logs.txt", med_privacy_dir / METADATA_DIR / f"{job.action}.log"
    )

    # Delete outputs from previous run of action
    existing_files = list_outputs_from_action(
        job.workspace, job.action, ignore_errors=True
    )
    files_to_remove = set(existing_files) - set(outputs)
    delete_files(high_privacy_dir, files_to_remove)
    delete_files(med_privacy_dir, files_to_remove)

    # Update manifest
    manifest = read_manifest_file(high_privacy_dir)
    update_manifest(manifest, job_metadata)
    write_manifest_file(high_privacy_dir, manifest)
    write_manifest_file(med_privacy_dir, manifest)

    if error:
        raise error


def cleanup_job(job):
    log.info("Deleting container and volume")
    docker.delete_container(container_name(job))
    docker.delete_volume(volume_name(job))


def get_container_metadata(job):
    metadata = docker.container_inspect(container_name(job), none_if_not_exists=True)
    if not metadata:
        raise JobError("Job container has vanished")
    redact_environment_variables(metadata)
    return metadata


def find_matching_outputs(job):
    """
    Returns a dict mapping output filenames to their privacy level, plus a list
    of any patterns that had no matches at all
    """
    all_patterns = []
    for privacy_level, named_patterns in job.output_spec.items():
        for name, pattern in named_patterns.items():
            all_patterns.append(pattern)
    all_matches = docker.glob_volume_files(volume_name(job), all_patterns)
    unmatched_patterns = []
    outputs = {}
    for privacy_level, named_patterns in job.output_spec.items():
        for name, pattern in named_patterns.items():
            filenames = all_matches[pattern]
            if not filenames:
                unmatched_patterns.append(pattern)
            for filename in filenames:
                outputs[filename] = privacy_level
    return outputs, unmatched_patterns


def get_job_metadata(job, container_metadata, outputs, error):
    """
    Returns a JSON-serializable dict including everything we know about a job
    """
    # There's a structural infelicity here that's hard to work around: what we
    # need to write to disk is the final state of the job (i.e. did it succeed
    # or fail and how long did it take). But the job won't transition into its
    # final state until after we've finished writing all the outputs and return
    # to the main run loop. (And it's important that only the main run loop
    # gets to set the job's state.) So there's a little bit of duplication of
    # the logic in `jobrunner.run` here to anticpate what the final state will
    # be.
    final_job = copy.copy(job)
    if error:
        final_job.status = State.FAILED
        final_job.status_message = f"{type(error).__name__}: {error}"
    else:
        final_job.status = State.SUCCEEDED
        final_job.status_message = "Completed successfully"
    final_job.completed_at = int(time.time())
    job_metadata = final_job.asdict()
    job_request = find_where(SavedJobRequest, id=final_job.job_request_id)[0]
    job_metadata["job_request"] = job_request.original
    job_metadata["job_id"] = job_metadata["id"]
    job_metadata["run_by_user"] = job_metadata["job_request"].get("created_by")
    job_metadata["docker_image_id"] = container_metadata["Image"]
    job_metadata["outputs"] = outputs
    job_metadata["container_metadata"] = container_metadata
    return job_metadata


def write_log_file(job, job_metadata, filename):
    """
    This dumps the (timestamped) Docker logs for a job to disk, followed by
    some useful metadata about the job and its outputs
    """
    filename.parent.mkdir(parents=True, exist_ok=True)
    docker.write_logs_to_file(container_name(job), filename)
    sorted_outputs = sorted(
        (privacy_level, name)
        for (name, privacy_level) in job_metadata["outputs"].items()
    )
    with open(filename, "a") as f:
        f.write("\n\n")
        for key in [
            "status",
            "status_message",
            "commit",
            "docker_image_id",
            "job_id",
            "run_by_user",
            "created_at",
            "started_at",
            "completed_at",
        ]:
            f.write(f"{key}: {job_metadata[key]}\n")
        f.write("\noutputs:\n")
        for privacy_level, name in sorted_outputs:
            f.write(f"  {privacy_level} - {name}\n")


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


def get_log_dir(job):
    # Split log directory up by month to make things slightly more manageable
    month_dir = datetime.date.today().strftime("%Y-%m")
    return config.JOB_LOG_DIR / month_dir / container_name(job)


def copy_file(source, dest):
    dest.parent.mkdir(parents=True, exist_ok=True)
    # shutil.copy() should be reasonably efficient in Python 3.8+, but if we
    # need to stick with 3.7 for some reason we could replace this with a
    # shellout to `cp`. See:
    # https://docs.python.org/3/library/shutil.html#shutil-platform-dependent-efficient-copy-operations
    shutil.copy(source, dest)


def delete_files(directory, filenames):
    for filename in filenames:
        try:
            directory.joinpath(filename).unlink()
        # On py3.8 we can use the `missing_ok=True` argument to unlink()
        except FileNotFoundError:
            pass


def outputs_exist(workspace, action):
    try:
        list_outputs_from_action(workspace, action)
        return True
    except MissingOutputError:
        return False


def list_outputs_from_action(workspace, action, ignore_errors=False):
    directory = config.HIGH_PRIVACY_WORKSPACES_DIR / workspace
    files = {}
    try:
        manifest = read_manifest_file(directory)
        files = manifest["files"]
        success = manifest["actions"][action]["status"] == State.SUCCEEDED.name.lower()
    except KeyError:
        success = False
    if not ignore_errors and not success:
        raise MissingOutputError(f"No successful outputs from {action}")
    output_files = []
    for filename, details in files.items():
        if details["created_by_action"] == action:
            output_files.append(filename)
            # This would only happen if files were manually deleted from disk
            if not ignore_errors and not directory.joinpath(filename).exists():
                raise MissingOutputError(f"Output {filename} missing from {action}")
    return output_files


def read_manifest_file(workspace_dir):
    """
    Read the manifest of a given workspace, returning an empty manifest if none
    found
    """
    try:
        with open(workspace_dir / METADATA_DIR / MANIFEST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"files": {}, "actions": {}}


def update_manifest(manifest, job_metadata):
    action = job_metadata["action"]
    # Popping and re-adding means it gets moved to the end of the dict so
    # actions end up in the order they were run
    manifest["actions"].pop(action, None)
    manifest["actions"][action] = {
        key: job_metadata[key]
        for key in [
            "status",
            "commit",
            "docker_image_id",
            "job_id",
            "run_by_user",
            "created_at",
            "completed_at",
        ]
    }
    # Remove all files created by previous runs of this action
    files = [
        (name, details)
        for (name, details) in manifest["files"].items()
        if details["created_by_action"] != action
    ]
    # Add newly created files
    for filename, privacy_level in job_metadata["outputs"].items():
        files.append(
            (filename, {"created_by_action": action, "privacy_level": privacy_level},)
        )
    files.sort()
    manifest["files"] = dict(files)


def write_manifest_file(workspace_dir, manifest):
    manifest_file = workspace_dir / METADATA_DIR / MANIFEST_FILE
    manifest_file_tmp = manifest_file.with_suffix(".tmp")
    manifest_file_tmp.write_text(json.dumps(manifest, indent=2))
    manifest_file_tmp.rename(manifest_file)
