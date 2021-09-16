import datetime
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Tuple, Optional

from jobrunner import config
from jobrunner.job_executor import Privacy, JobAPI, WorkspaceAPI, JobResults, JobDefinition
from jobrunner.lib import docker
from jobrunner.lib.git import checkout_commit
from jobrunner.lib.string_utils import tabulate
from jobrunner.lib.subprocess_utils import subprocess_run
from jobrunner.models import State, StatusCode, JobError

log = logging.getLogger(__name__)


class LocalDockerJobAPI(JobAPI):
    def run(self, job: JobDefinition) -> None:
        try:
            # Check the image exists locally and error if not. Newer versions of
            # docker-cli support `--pull=never` as an argument to `docker run` which
            # would make this simpler, but it looks like it will be a while before this
            # makes it to Docker for Windows:
            # https://github.com/docker/cli/pull/1498
            if not docker.image_exists_locally(job.image):
                log.info(f"Image not found, may need to run: docker pull {job.image}")
                raise JobError(f"Docker image {job.image} is not currently available")
            # If we already created the job but were killed before we updated the state
            # then there's nothing further to do
            if docker.container_exists(container_name(job.id)):
                log.info("Container already created, nothing to do")
            else:
                start_container(job)
                log.info("Started")
                log.info(f"View live logs using: docker logs -f {container_name(job.id)}")
        except Exception as exception:
            # See the `raise` below which explains why we can't
            # cleanup on this specific error
            if not isinstance(exception, BrokenContainerError):
                cleanup_job(job.id)
            raise

    def terminate(self, job):
        docker.kill(container_name(job.id))

    def get_status(self, job) -> Tuple[State, Optional[JobResults]]:
        if job_still_running(job.id):
            return State.RUNNING, None
        else:
            return finalize_job(job)


class LocalDockerWorkspaceAPI(WorkspaceAPI):
    def delete_files(self, workspace, privacy, paths):
        if privacy == Privacy.HIGH:
            directory = get_high_privacy_workspace(workspace)
        else:
            directory = get_medium_privacy_workspace(workspace)
        ensure_overwritable(*[directory.joinpath(f) for f in paths])
        # TODO Worry about case-sensitivity of filenames
        for filename in paths:
            path = directory / filename
            path.unlink()


# This is a Docker label applied in addition to the default label which
# `docker.py` applies to all containers and volumes it creates. It allows us to
# easily identify just the containers actually used for running jobs, which is
# helpful for building tooling for inspecting live processes.
JOB_LABEL = "jobrunner-job"


def start_container(job: JobDefinition):
    try:
        volume = create_and_populate_volume(job.id, job.workspace, job.inputs,
                                            job.study.git_repo_url, job.study.commit,
                                            job.output_spec)
    except docker.DockerDiskSpaceError as e:
        log.exception(str(e))
        raise JobError("Out of disk space, please try again later")
    # Start the container
    docker.run(
        container_name(job.id),
        [job.image] + job.args,
        volume=(volume, "/workspace"),
        env=job.env,
        allow_network_access=job.allow_database_access,
        label=JOB_LABEL,
    )


def finalize_job(job: JobDefinition) -> Tuple[State, Optional[JobResults]]:
    try:
        container_metadata = get_container_metadata(job.id)
        outputs, unmatched_patterns = find_matching_outputs(job.id)
        # Set the final state of the job
        status_code = None
        if container_metadata["State"]["ExitCode"] != 0:
            state = State.FAILED
            status_message = "Job exited with an error code"
            status_code = StatusCode.NONZERO_EXIT
        elif unmatched_patterns:
            # If the job fails because an output was missing its very useful to
            # show the user what files were created as often the issue is just a
            # typo
            unmatched_outputs = get_unmatched_outputs(job.id, outputs)
            state = State.FAILED
            status_message = """
                No outputs found matching patterns:
                 - {}
                Did you mean to match one of these files instead?
                 - {}
                """.format("\n - ".join(unmatched_patterns), "\n - ".join(unmatched_outputs))
        else:
            state = State.SUCCEEDED
            status_message = "Completed successfully"
        # job_metadata is a big dict capturing everything we know about the state
        # of the job
        job_metadata = get_job_metadata(container_metadata)
        # Dump useful info in log directory
        log_dir = get_log_dir(job.id)
        ensure_overwritable(log_dir / "logs.txt", log_dir / "metadata.json")
        write_log_file(job.id, job_metadata, log_dir / "logs.txt")
        with open(log_dir / "metadata.json", "w") as f:
            json.dump(job_metadata, f, indent=2)
        # Copy logs to workspace
        workspace_dir = get_high_privacy_workspace(job.workspace)
        metadata_log_file = workspace_dir / METADATA_DIR / f"{job.action}.log"
        copy_file(log_dir / "logs.txt", metadata_log_file)
        log.info(f"Logs written to: {metadata_log_file}")
        # Extract outputs to workspace
        ensure_overwritable(*[workspace_dir / f for f in outputs.keys()])
        volume = volume_name(job.id)
        for filename in outputs.keys():
            log.info(f"Extracting output file: {filename}")
            docker.copy_from_volume(volume, filename, workspace_dir / filename)
        # Copy out logs and medium privacy files
        medium_privacy_dir = get_medium_privacy_workspace(job.workspace)
        if medium_privacy_dir:
            copy_file(
                workspace_dir / METADATA_DIR / f"{job.action}.log",
                medium_privacy_dir / METADATA_DIR / f"{job.action}.log",
            )
            for filename, privacy_level in outputs.items():
                if privacy_level == "moderately_sensitive":
                    copy_file(workspace_dir / filename, medium_privacy_dir / filename)
        cleanup_job(job.id)
    except JobError:
        cleanup_job(job.id)
        raise
    return state, JobResults(state, status_code, status_message, outputs)


def job_still_running(job_id):
    return docker.container_is_running(container_name(job_id))


def cleanup_job(job_id):
    if config.CLEAN_UP_DOCKER_OBJECTS:
        log.info("Cleaning up container and volume")
        docker.delete_container(container_name(job_id))
        docker.delete_volume(volume_name(job_id))
    else:
        log.info("Leaving container and volume in place for debugging")


def create_and_populate_volume(job_id, workspace, input_files, repo_url, commit, output_spec):
    workspace_dir = get_high_privacy_workspace(workspace)

    volume = volume_name(job_id)
    docker.create_volume(volume, output_spec)

    # `docker cp` can't create parent directories for us so we make sure all
    # these directories get created when we copy in the code
    extra_dirs = set(Path(filename).parent for filename in input_files)

    copy_git_commit_to_volume(volume, repo_url, commit, extra_dirs)

    for filename in input_files:
        log.info(f"Copying input file: {filename}")
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


def container_name(slug):
    return f"job-{slug}"


def volume_name(job_id):
    return f"volume-{job_id}"


def get_high_privacy_workspace(workspace):
    return config.HIGH_PRIVACY_WORKSPACES_DIR / workspace


# Directory inside working directory where manifest and logs are created
METADATA_DIR = "metadata"

# This is part of a hack we use to track which files in a volume are newly
# created
TIMESTAMP_REFERENCE_FILE = ".opensafely-timestamp"


def get_container_metadata(job_id):
    metadata = docker.container_inspect(container_name(job_id), none_if_not_exists=True)
    if not metadata:
        raise JobError("Job container has vanished")
    redact_environment_variables(metadata)
    return metadata


def find_matching_outputs(job_id):
    """
    Returns a dict mapping output filenames to their privacy level, plus a list
    of any patterns that had no matches at all
    """
    all_matches, output_spec = docker.glob_volume_files(volume_name(job_id))
    unmatched_patterns = []
    outputs = {}
    for pattern, privacy_level in output_spec.items():
        filenames = all_matches[pattern]
        if not filenames:
            unmatched_patterns.append(pattern)
        for filename in filenames:
            outputs[filename] = privacy_level
    return outputs, unmatched_patterns


def get_unmatched_outputs(job_id, outputs):
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
    all_outputs = docker.find_newer_files(volume_name(job_id), TIMESTAMP_REFERENCE_FILE)
    return [filename for filename in all_outputs if filename not in outputs]


def get_job_metadata(container_metadata):
    """
    Returns a JSON-serializable dict including everything we know about a job
    """
    # TODO We need separate mechanisms for persisting detailed debug information inside and outside the job-executor.
    #      Here we are inside, so we don't have the job and job request metadata.
    # # This won't exactly match the final `completed_at` time which doesn't get
    # # set until the entire job has finished processing, but we want _some_ kind
    # # of time to put in the metadata
    # job.completed_at = int(time.time())
    # job_metadata = job.asdict()
    # job_request = find_where(SavedJobRequest, id=job.job_request_id)[0]
    # # The original job_request, exactly as received from the job-server
    # job_metadata["job_request"] = job_request.original
    # job_metadata["job_id"] = job_metadata["id"]
    # job_metadata["run_by_user"] = job_metadata["job_request"].get("created_by")
    job_metadata = dict()
    job_metadata["docker_image_id"] = container_metadata["Image"]
    job_metadata["exit_code"] = container_metadata["State"]["ExitCode"]
    job_metadata["container_metadata"] = container_metadata
    return job_metadata


def write_log_file(job_id, job_metadata, filename):
    """
    This dumps the (timestamped) Docker logs for a job to disk, followed by
    some useful metadata about the job and its outputs
    """
    filename.parent.mkdir(parents=True, exist_ok=True)
    docker.write_logs_to_file(container_name(job_id), filename)
    outputs = sorted(job_metadata["outputs"].items())
    with open(filename, "a") as f:
        f.write("\n\n")
        # TODO Some of these keys are not be available here, inside the job-executor.
        for key in KEYS_TO_LOG:
            if not job_metadata[key]:
                continue
            f.write(f"{key}: {job_metadata[key]}\n")
        f.write(f"\n{job_metadata['status_message']}\n")
        f.write("\noutputs:\n")
        f.write(tabulate(outputs, separator="  - ", indent=2, empty="(no outputs)"))


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


def get_log_dir(job_id):
    # Split log directory up by month to make things slightly more manageable
    month_dir = datetime.date.today().strftime("%Y-%m")
    return config.JOB_LOG_DIR / month_dir / container_name(job_id)


def copy_file(source, dest):
    dest.parent.mkdir(parents=True, exist_ok=True)
    ensure_overwritable(dest)
    # shutil.copy() should be reasonably efficient in Python 3.8+, but if we
    # need to stick with 3.7 for some reason we could replace this with a
    # shellout to `cp`. See:
    # https://docs.python.org/3/library/shutil.html#shutil-platform-dependent-efficient-copy-operations
    shutil.copy(source, dest)


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


class BrokenContainerError(JobError):
    pass
