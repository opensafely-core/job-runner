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
import os.path
from pathlib import Path
import shlex
import shutil
import tempfile
import time

from . import config
from . import docker
from .database import find_where
from .git import checkout_commit
from .models import SavedJobRequest, State, StatusCode
from .project import (
    is_generate_cohort_command,
    get_all_output_patterns_from_project_file,
)
from .path_utils import list_dir_with_ignore_patterns
from .string_utils import tabulate
from .subprocess_utils import subprocess_run


log = logging.getLogger(__name__)

# Directory inside working directory where manifest and logs are created
METADATA_DIR = "metadata"

# Records details of which action created each file
MANIFEST_FILE = "manifest.json"

# This is a Docker label applied in addition to the default label which
# `docker.py` applies to all containers and volumes it creates. It allows us to
# easily identify just the containers actually used for running jobs, which is
# helpful for building tooling for inspecting live processes.
JOB_LABEL = "jobrunner-job"

# This is part of a hack we use to track which files in a volume are newly
# created
TIMESTAMP_REFERENCE_FILE = ".opensafely-timestamp"


class JobError(Exception):
    pass


class ActionNotRunError(JobError):
    pass


class ActionFailedError(JobError):
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
    if image.startswith("stata-mp"):
        env["STATA_LICENSE"] = str(config.STATA_LICENSE)
    # Check the image exists locally and error if not. Newer versions of
    # docker-cli support `--pull=never` as an argument to `docker run` which
    # would make this simpler, but it looks like it will be a while before this
    # makes it to Docker for Windows:
    # https://github.com/docker/cli/pull/1498
    if not docker.image_exists_locally(full_image):
        log.info(f"Image not found, may need to run: docker pull {full_image}")
        raise JobError(f"Docker image {image} is not currently available")
    # Start the container
    docker.run(
        container_name(job),
        [full_image] + action_args[1:],
        volume=(volume, "/workspace"),
        env=env,
        allow_network_access=allow_network_access,
        label=JOB_LABEL,
    )
    log.info("Started")
    log.info(f"View live logs using: docker logs -f {container_name(job)}")


def create_and_populate_volume(job):
    workspace_dir = get_high_privacy_workspace(job.workspace)
    input_files = {}
    for action in job.requires_outputs_from:
        for filename in list_outputs_from_action(job.workspace, action):
            input_files[filename] = action

    volume = volume_name(job)
    docker.create_volume(volume)

    # `docker cp` can't create parent directories for us so we make sure all
    # these directories get created when we copy in the code
    extra_dirs = set(Path(filename).parent for filename in input_files.keys())
    if config.LOCAL_RUN_MODE:
        copy_local_workspace_to_volume(volume, workspace_dir, extra_dirs)
    else:
        copy_git_commit_to_volume(volume, job.repo_url, job.commit, extra_dirs)

    for filename, action in input_files.items():
        log.info(f"Copying input file {action}: {filename}")
        docker.copy_to_volume(volume, workspace_dir / filename, filename)
    # Hack: see `get_unmatched_outputs`. For some reason this requires a
    # non-empty file so copying `os.devnull` didn't work.
    some_non_empty_file = Path(__file__)
    docker.copy_to_volume(volume, some_non_empty_file, TIMESTAMP_REFERENCE_FILE)
    return volume


def copy_git_commit_to_volume(volume, repo_url, commit, extra_dirs):
    log.info(f"Copying in code from {repo_url}@{commit}")
    # git-archive will create a tarball on stdout and docker cp will accept a
    # tarball on stdin, so if we wanted to we could do this all without a
    # temporary directory, but not worth it at this stage
    config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=config.TMP_DIR) as tmpdir:
        tmpdir = Path(tmpdir)
        checkout_commit(repo_url, commit, tmpdir)
        # Because `docker cp` can't create parent directories automatically, we
        # make sure parent directories exist for all the files we're going to
        # copy in later
        for directory in extra_dirs:
            tmpdir.joinpath(directory).mkdir(parents=True, exist_ok=True)
        docker.copy_to_volume(volume, tmpdir, ".")


def copy_local_workspace_to_volume(volume, workspace_dir, extra_dirs):
    # To mimic a production run, we only want output files to appear in the
    # volume if they were produced by an explicitly listed dependency. So
    # before copying in the code we get a list of all output patterns in the
    # project and ignore any files matching these patterns
    project_file = workspace_dir / "project.yaml"
    ignore_patterns = get_all_output_patterns_from_project_file(project_file)
    ignore_patterns.extend([".git", METADATA_DIR])
    code_files = list_dir_with_ignore_patterns(workspace_dir, ignore_patterns)

    # Because `docker cp` can't create parent directories automatically, we
    # need to make sure empty parent directories exist for all the files we're
    # going to copy in. For now we do this by actually creating a bunch of
    # empty dirs in a temp directory. It should be possible to do this using
    # the `tarfile` module to talk directly to `docker cp` stdin if we care
    # enough.
    directories = set(Path(filename).parent for filename in code_files)
    directories.update(extra_dirs)
    directories.discard(Path("."))
    if directories:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            for directory in directories:
                tmpdir.joinpath(directory).mkdir(parents=True, exist_ok=True)
            docker.copy_to_volume(volume, tmpdir, ".")

    log.info(f"Copying in code from {workspace_dir}")
    for filename in code_files:
        docker.copy_to_volume(volume, workspace_dir / filename, filename)


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
    job.outputs = outputs

    # Set the final state of the job
    if container_metadata["State"]["ExitCode"] != 0:
        job.state = State.FAILED
        job.status_message = "Job exited with an error code"
        job.status_code = StatusCode.NONZERO_EXIT
    elif unmatched_patterns:
        job.state = State.FAILED
        job.status_message = "No outputs found matching patterns:\n - {}".format(
            "\n - ".join(unmatched_patterns)
        )
        # If the job fails because an output was missing its very useful to
        # show the user what files were created as often the issue is just a
        # typo
        job.unmatched_outputs = get_unmatched_outputs(job)
    else:
        job.state = State.SUCCEEDED
        job.status_message = "Completed successfully"

    # job_metadata is a big dict capturing everything we know about the state
    # of the job
    job_metadata = get_job_metadata(job, container_metadata)

    # Dump useful info in log directory
    log_dir = get_log_dir(job)
    ensure_overwritable(log_dir / "logs.txt", log_dir / "metadata.json")
    write_log_file(job, job_metadata, log_dir / "logs.txt")
    with open(log_dir / "metadata.json", "w") as f:
        json.dump(job_metadata, f, indent=2)

    # Copy logs to workspace
    workspace_dir = get_high_privacy_workspace(job.workspace)
    metadata_log_file = workspace_dir / METADATA_DIR / f"{job.action}.log"
    copy_file(log_dir / "logs.txt", metadata_log_file)
    log.info(f"Logs written to: {metadata_log_file}")

    # Extract outputs to workspace
    ensure_overwritable(*[workspace_dir / f for f in job.outputs.keys()])
    volume = volume_name(job)
    for filename in job.outputs.keys():
        log.info(f"Extracting output file: {filename}")
        docker.copy_from_volume(volume, filename, workspace_dir / filename)

    # Delete outputs from previous run of action
    existing_files = list_outputs_from_action(
        job.workspace, job.action, ignore_errors=True
    )
    files_to_remove = set(existing_files) - set(job.outputs)
    delete_files(workspace_dir, files_to_remove)

    # Update manifest
    manifest = read_manifest_file(workspace_dir)
    update_manifest(manifest, job_metadata)

    # Copy out logs and medium privacy files
    medium_privacy_dir = get_medium_privacy_workspace(job.workspace)
    if medium_privacy_dir:
        copy_file(
            workspace_dir / METADATA_DIR / f"{job.action}.log",
            medium_privacy_dir / METADATA_DIR / f"{job.action}.log",
        )
        for filename, privacy_level in job.outputs.items():
            if privacy_level == "moderately_sensitive":
                copy_file(workspace_dir / filename, medium_privacy_dir / filename)
        delete_files(medium_privacy_dir, files_to_remove)
        write_manifest_file(medium_privacy_dir, manifest)

    # Don't update the primary manifest until after we've deleted old files
    # from both the high and medium privacy directories, else we risk losing
    # track of old files if we get interrupted
    write_manifest_file(workspace_dir, manifest)

    return job


def cleanup_job(job):
    log.info("Cleaning up container and volume")
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


def get_unmatched_outputs(job):
    """
    Returns all the files created by the job which were *not* matched by any of
    the output patterns.

    This is very useful in debugging because it's easy for users to get their
    output patterns wrong (often just in the wrong directory) and it becomes
    immediately obvious what the problem is if, as well as an error, they can
    see a list of the files the job *did* produce.

    The way we do this is bit hacky, but given that it's only used for
    debugging info and not for Serious Business Purposes, it should be
    sufficient.
    """
    all_outputs = docker.find_newer_files(volume_name(job), TIMESTAMP_REFERENCE_FILE)
    return [filename for filename in all_outputs if filename not in job.outputs]


def get_job_metadata(job, container_metadata):
    """
    Returns a JSON-serializable dict including everything we know about a job
    """
    # This won't exactly match the final `completed_at` time which doesn't get
    # set until the entire job has finished processing, but we want _some_ kind
    # of time to put in the metadata
    job.completed_at = int(time.time())
    job_metadata = job.asdict()
    job_request = find_where(SavedJobRequest, id=job.job_request_id)[0]
    # The original job_request, exactly as received from the job-server
    job_metadata["job_request"] = job_request.original
    job_metadata["job_id"] = job_metadata["id"]
    job_metadata["run_by_user"] = job_metadata["job_request"].get("created_by")
    job_metadata["docker_image_id"] = container_metadata["Image"]
    job_metadata["container_metadata"] = container_metadata
    return job_metadata


def write_log_file(job, job_metadata, filename):
    """
    This dumps the (timestamped) Docker logs for a job to disk, followed by
    some useful metadata about the job and its outputs
    """
    filename.parent.mkdir(parents=True, exist_ok=True)
    docker.write_logs_to_file(container_name(job), filename)
    outputs = sorted(job_metadata["outputs"].items())
    with open(filename, "a") as f:
        f.write("\n\n")
        for key in [
            "state",
            "commit",
            "docker_image_id",
            "job_id",
            "run_by_user",
            "created_at",
            "started_at",
            "completed_at",
        ]:
            f.write(f"{key}: {job_metadata[key]}\n")
        f.write(f"\n{job_metadata['status_message']}\n")
        if job.unmatched_outputs:
            f.write("\nDid you mean to match one of these files instead?\n - ")
            f.write("\n - ".join(job.unmatched_outputs))
            f.write("\n")
        f.write("\noutputs:\n")
        f.write(tabulate(outputs, separator="  - ", indent=2, empty="(no outputs)"))


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
    ensure_overwritable(dest)
    # shutil.copy() should be reasonably efficient in Python 3.8+, but if we
    # need to stick with 3.7 for some reason we could replace this with a
    # shellout to `cp`. See:
    # https://docs.python.org/3/library/shutil.html#shutil-platform-dependent-efficient-copy-operations
    shutil.copy(source, dest)


def delete_files(directory, filenames):
    ensure_overwritable(*[directory.joinpath(f) for f in filenames])
    for filename in filenames:
        try:
            directory.joinpath(filename).unlink()
        # On py3.8 we can use the `missing_ok=True` argument to unlink()
        except FileNotFoundError:
            pass


def action_has_successful_outputs(workspace, action):
    """
    Returns True if the action ran successfully and all its outputs still exist
    on disk.
    Returns False if the action was run and failed.
    Returns None if the action hasn't been run yet.

    If an action _has_ run, but some of its files have been manually deleted
    from disk we treat this as equivalent to not being run i.e. there was no
    explicit failure with the action, but we can't treat it as having
    successful outputs either.
    """
    try:
        list_outputs_from_action(workspace, action)
        return True
    except ActionFailedError:
        return False
    except (ActionNotRunError, MissingOutputError):
        return None


def list_outputs_from_action(workspace, action, ignore_errors=False):
    directory = get_high_privacy_workspace(workspace)
    files = {}
    try:
        manifest = read_manifest_file(directory)
        files = manifest["files"]
        state = manifest["actions"][action]["state"]
    except KeyError:
        state = None
    if not ignore_errors:
        if state is None:
            raise ActionNotRunError(f"{action} has not been run")
        if state != State.SUCCEEDED.value:
            raise ActionFailedError(f"{action} failed")
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
    new_outputs = job_metadata["outputs"]
    # Remove all files created by previous runs of this action, and any files
    # created by other actions which are being overwritten by this action. This
    # latter case should never occur during a "clean" run of a project because
    # each output file should be unique across the project. However when
    # iterating during development it's possible to move outputs between
    # actions and hit this condition.
    files = [
        (name, details)
        for (name, details) in manifest["files"].items()
        if details["created_by_action"] != action and name not in new_outputs
    ]
    # Add newly created files
    for filename, privacy_level in new_outputs.items():
        files.append(
            (
                filename,
                {"created_by_action": action, "privacy_level": privacy_level},
            )
        )
    files.sort()
    manifest["files"] = dict(files)
    # Popping and re-adding means the action gets moved to the end of the dict
    # so actions end up in the order they were run
    manifest["actions"].pop(action, None)
    manifest["actions"][action] = {
        key: job_metadata[key]
        for key in [
            "state",
            "commit",
            "docker_image_id",
            "job_id",
            "run_by_user",
            "created_at",
            "completed_at",
        ]
    }


def write_manifest_file(workspace_dir, manifest):
    manifest_file = workspace_dir / METADATA_DIR / MANIFEST_FILE
    manifest_file_tmp = manifest_file.with_suffix(".tmp")
    ensure_overwritable(manifest_file, manifest_file_tmp)
    manifest_file_tmp.write_text(json.dumps(manifest, indent=2))
    manifest_file_tmp.replace(manifest_file)


def get_high_privacy_workspace(workspace):
    return config.HIGH_PRIVACY_WORKSPACES_DIR / workspace


def get_medium_privacy_workspace(workspace):
    if config.MEDIUM_PRIVACY_WORKSPACES_DIR:
        return config.MEDIUM_PRIVACY_WORKSPACES_DIR / workspace
    else:
        return None


def ensure_overwritable(*paths):
    """
    This is a (nasty) workaround for the permissions issues we hit when
    switching between running the job-runner inside Docker and running it
    natively on Windows. The issue is that the Docker process creates files
    which the Windows native process then doesn't have permission to delete or
    replace. We work around this here by using Docker to delete the offending
    files for us.

    Note for the potentially confused: Windows permissions work nothing like
    POSIX permissions. We can create new files in directories created by
    Docker, we just can't modify or delete existing files.
    """
    if not config.ENABLE_PERMISSIONS_WORKAROUND:
        return
    non_writable = []
    for path in paths:
        path = Path(path)
        if path.exists():
            # It would be nice to have a read-only way of determining if we
            # have write access but I can't seem to find one that works on
            # Windows
            try:
                path.touch()
            except PermissionError:
                non_writable.append(path.resolve())
    if not non_writable:
        return
    root = os.path.commonpath([f.parent for f in non_writable])
    rel_paths = [f.relative_to(root) for f in non_writable]
    rel_posix_paths = [str(f).replace(os.path.sep, "/") for f in rel_paths]
    subprocess_run(
        [
            "docker",
            "run",
            "--rm",
            "--volume",
            f"{root}:/workspace",
            "--workdir",
            "/workspace",
            docker.MANAGEMENT_CONTAINER_IMAGE,
            "rm",
            *rel_posix_paths,
        ],
        check=True,
        capture_output=True,
    )
