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
import shlex
import shutil
import tempfile
import time
from pathlib import Path

from jobrunner import config
from jobrunner.lib import docker
from jobrunner.lib.database import find_one
from jobrunner.lib.git import checkout_commit
from jobrunner.lib.path_utils import list_dir_with_ignore_patterns
from jobrunner.lib.string_utils import tabulate
from jobrunner.lib.subprocess_utils import subprocess_run
from jobrunner.models import SavedJobRequest, State, StatusCode
from jobrunner.project import (
    get_all_output_patterns_from_project_file,
    is_generate_cohort_command,
)
from jobrunner.queries import calculate_workspace_state


log = logging.getLogger(__name__)

# Directory inside working directory where manifest and logs are created
METADATA_DIR = "metadata"

# Records details of which action created each file
MANIFEST_FILE = "manifest.json"

# Keys of fields to log in manifest.json and log file
KEYS_TO_LOG = [
    "state",
    "commit",
    "docker_image_id",
    "action_repo_url",
    "action_commit",
    "job_id",
    "run_by_user",
    "created_at",
    "completed_at",
    "exit_code",
]


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


class BrokenContainerError(JobError):
    pass


def start_job(job):
    """Start the given job.

    Args:
        job: An instance of Job.
    """
    # If we already created the job but were killed before we updated the state
    # then there's nothing further to do
    if docker.container_exists(container_name(job)):
        log.info("Container already created, nothing to do")
        return
    try:
        volume = create_and_populate_volume(job)
    except docker.DockerDiskSpaceError as e:
        log.exception(str(e))
        raise JobError("Out of disk space, please try again later")
    action_args = shlex.split(job.run_command)
    allow_network_access = False
    env = {"OPENSAFELY_BACKEND": config.BACKEND}
    # Check `is True` so we fail closed if we ever get anything else
    if is_generate_cohort_command(action_args) is True:
        if not config.USING_DUMMY_DATA_BACKEND:
            allow_network_access = True
            env["DATABASE_URL"] = config.DATABASE_URLS[job.database_name]
            if config.TEMP_DATABASE_NAME:
                env["TEMP_DATABASE_NAME"] = config.TEMP_DATABASE_NAME
            if config.PRESTO_TLS_KEY and config.PRESTO_TLS_CERT:
                env["PRESTO_TLS_CERT"] = config.PRESTO_TLS_CERT
                env["PRESTO_TLS_KEY"] = config.PRESTO_TLS_KEY
            if config.EMIS_ORGANISATION_HASH:
                env["EMIS_ORGANISATION_HASH"] = config.EMIS_ORGANISATION_HASH
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

    # Jobs which are running reusable actions pull their code from the reusable
    # action repo, all other jobs pull their code from the study repo
    repo_url = job.action_repo_url or job.repo_url
    commit = job.action_commit or job.commit
    # Both of action commit and repo_url should be set if either are
    assert bool(job.action_commit) == bool(job.action_repo_url)

    if repo_url and commit:
        copy_git_commit_to_volume(volume, repo_url, commit, extra_dirs)
    else:
        # We only encounter jobs without a repo or commit when using the
        # "local_run" command to execute uncommitted local code
        copy_local_workspace_to_volume(volume, workspace_dir, extra_dirs)

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
        try:
            docker.copy_to_volume(volume, tmpdir, ".", timeout=60)
        except docker.DockerTimeoutError:
            # Aborting a `docker cp` into a container at the wrong time can
            # leave the container in a completely broken state where any
            # attempt to interact with or even remove it will just hang, see:
            # https://github.com/docker/for-mac/issues/4491
            #
            # This means we can end up with jobs where any attempt to start
            # them (by copying in code from git) causes the job-runner to
            # completely lock up. To avoid this we use a timeout (60 seconds,
            # which should be more than enough to copy in a few megabytes of
            # code). The exception this triggers will cause the job to fail
            # with an "internal error" message, which will then stop it
            # blocking other jobs. We need a specific exception class here as
            # we need to avoid trying to remove the container, which we would
            # ordinarily do on error, because that operation will also hang :(
            log.exception("Timed out copying code to volume, see issue #154")
            raise BrokenContainerError(
                "There was a (hopefully temporary) internal Docker error, "
                "please try the job again"
            )


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


# note: these functions use different naming logic for to isolate the state between the old/new worlds
def container_name(job):
    return f"os-job-{job.id}" if config.EXECUTION_API else f"job-{job.slug}"


def volume_name(job):
    return f"os-volume-{job.id}" if config.EXECUTION_API else f"volume-{job.slug}"


def finalise_job(job):
    """
    This involves checking whether the job finished successfully or not and
    extracting all outputs, logs and metadata
    """
    container_metadata = get_container_metadata(job)
    outputs, unmatched_patterns = find_matching_outputs(job)
    job.outputs = outputs
    job.image_id = container_metadata["Image"]

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

    # If a job was cancelled we bail out before making any changes to the
    # workspace, but after having written the log and metadata files to the
    # long-term logs directory for debugging purposes.
    if job.cancelled:
        raise JobError("Cancelled by user")

    # Copy logs to workspace
    workspace_dir = get_high_privacy_workspace(job.workspace)
    metadata_log_file = workspace_dir / METADATA_DIR / f"{job.action}.log"
    copy_file(log_dir / "logs.txt", metadata_log_file)
    log.info(f"Logs written to: {metadata_log_file}")

    # Extract outputs to workspace
    ensure_overwritable(*[workspace_dir / f for f in job.output_files])
    volume = volume_name(job)
    for filename in job.output_files:
        log.info(f"Extracting output file: {filename}")
        docker.copy_from_volume(volume, filename, workspace_dir / filename)

    # Delete outputs from previous run of action. It would be simpler to delete
    # all existing outputs and then copy over the new ones, but this way we
    # don't delete anything until after we've copied the new outputs which is
    # safer in case anything goes wrong.
    existing_files = list_outputs_from_action(job.workspace, job.action)
    delete_files(workspace_dir, existing_files, files_to_keep=job.output_files)

    # Copy out logs and medium privacy files
    medium_privacy_dir = get_medium_privacy_workspace(job.workspace)
    if medium_privacy_dir:
        copy_file(
            workspace_dir / METADATA_DIR / f"{job.action}.log",
            medium_privacy_dir / METADATA_DIR / f"{job.action}.log",
        )
        new_files = []
        for filename, privacy_level in job.outputs.items():
            if privacy_level == "moderately_sensitive":
                copy_file(workspace_dir / filename, medium_privacy_dir / filename)
                new_files.append(filename)
        delete_files(medium_privacy_dir, existing_files, files_to_keep=new_files)

        # osrelease needs to be able to read the workspace name and repo URL from somewhere, in order to avoid the
        # person doing the release having to enter all the details. So we write this rump manifest just into the
        # medium privacy workspace. release-hatch is launched with this information already provided, so when osrelease
        # has been removed we can stop doing this.
        #
        # We only really need to write this the first time that an action is run in a workspace, but it's easier to do
        # it here every time than try to detect that.
        write_manifest_file(
            medium_privacy_dir, {"repo": job.repo_url, "workspace": job.workspace}
        )

    return job


def cleanup_job(job):
    if config.CLEAN_UP_DOCKER_OBJECTS:
        log.info("Cleaning up container and volume")
        docker.delete_container(container_name(job))
        docker.delete_volume(volume_name(job))
    else:
        log.info("Leaving container and volume in place for debugging")


def kill_job(job):
    docker.kill(container_name(job))


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
    job_request = find_one(SavedJobRequest, id=job.job_request_id)
    # The original job_request, exactly as received from the job-server
    job_metadata["job_request"] = job_request.original
    job_metadata["job_id"] = job_metadata["id"]
    job_metadata["run_by_user"] = job_metadata["job_request"].get("created_by")
    job_metadata["docker_image_id"] = container_metadata["Image"]
    job_metadata["exit_code"] = container_metadata["State"]["ExitCode"]
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
        for key in KEYS_TO_LOG:
            if not job_metadata[key]:
                continue
            f.write(f"{key}: {job_metadata[key]}\n")
        f.write(f"\n{job_metadata['status_message']}\n")
        if job.unmatched_outputs:
            f.write("\nDid you mean to match one of these files instead?\n - ")
            f.write("\n - ".join(job.unmatched_outputs))
            f.write("\n")
        f.write("\noutputs:\n")
        f.write(tabulate(outputs, separator="  - ", indent=2, empty="(no outputs)"))


# Environment variables whose values do not need to be hidden from the debug
# logs
SAFE_ENVIRONMENT_VARIABLES = set(
    """
    PATH PYTHON_VERSION DEBIAN_FRONTEND DEBCONF_NONINTERACTIVE_SEEN
    UBUNTU_VERSION PYENV_SHELL PYENV_VERSION PYTHONUNBUFFERED
    OPENSAFELY_BACKEND TZ TEMP_DATABASE_NAME PYTHONPATH container LANG LC_ALL
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


def delete_files(directory, filenames, files_to_keep=()):
    ensure_overwritable(*[directory.joinpath(f) for f in filenames])
    # We implement the "files to keep" logic using inodes rather than names so
    # we can safely handle case-insensitive filesystems
    inodes_to_keep = set()
    for filename in files_to_keep:
        try:
            stat = directory.joinpath(filename).stat()
            inodes_to_keep.add((stat.st_dev, stat.st_ino))
        except FileNotFoundError:
            pass
    for filename in filenames:
        path = directory / filename
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        inode = (stat.st_dev, stat.st_ino)
        if inode not in inodes_to_keep:
            path.unlink()


def list_outputs_from_action(workspace, action):
    for job in calculate_workspace_state(workspace):
        if job.action == action:
            return job.output_files

    # The action has never been run before
    return []


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
