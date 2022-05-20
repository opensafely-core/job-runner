import datetime
import json
import logging
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from jobrunner import config
from jobrunner.job_executor import (
    ExecutorAPI,
    ExecutorState,
    JobDefinition,
    JobResults,
    JobStatus,
    Privacy,
)
from jobrunner.lib import atomic_writer, docker
from jobrunner.lib.git import checkout_commit
from jobrunner.lib.path_utils import list_dir_with_ignore_patterns
from jobrunner.lib.string_utils import tabulate
from jobrunner.project import get_all_output_patterns_from_project_file


# Directory inside working directory where manifest and logs are created
METADATA_DIR = "metadata"

# Records details of which action created each file
MANIFEST_FILE = "manifest.json"

# This is part of a hack we use to track which files in a volume are newly
# created
TIMESTAMP_REFERENCE_FILE = ".opensafely-timestamp"

# cache of result objects
RESULTS = {}
LABEL = "jobrunner-local"

log = logging.getLogger(__name__)


def container_name(job):
    return f"os-job-{job.id}"


def volume_name(job):
    return f"os-volume-{job.id}"


def get_high_privacy_workspace(workspace):
    return config.HIGH_PRIVACY_WORKSPACES_DIR / workspace


def get_medium_privacy_workspace(workspace):
    if config.MEDIUM_PRIVACY_WORKSPACES_DIR:
        return config.MEDIUM_PRIVACY_WORKSPACES_DIR / workspace
    else:
        return None


def get_log_dir(job):
    # Split log directory up by month to make things slightly more manageable
    month_dir = datetime.date.today().strftime("%Y-%m")
    return config.JOB_LOG_DIR / month_dir / container_name(job)


class LocalDockerError(Exception):
    pass


def get_job_labels(job: JobDefinition):
    """Useful metadata to label docker objects with."""
    return {
        "workspace": job.workspace,
        "action": job.action,
    }


def workspace_is_archived(workspace):
    archive_dir = config.HIGH_PRIVACY_ARCHIVE_DIR
    for ext in config.ARCHIVE_FORMATS:
        path = (archive_dir / workspace).with_suffix(ext)
        if path.exists():
            return True
    return False


class LocalDockerAPI(ExecutorAPI):
    """ExecutorAPI implementation using local docker service."""

    def prepare(self, job):
        current = self.get_status(job)
        if current.state != ExecutorState.UNKNOWN:
            return current

        # Check the workspace is not archived
        workspace_dir = get_high_privacy_workspace(job.workspace)
        if not workspace_dir.exists():
            if workspace_is_archived(job.workspace):
                return JobStatus(
                    ExecutorState.ERROR,
                    f"Workspace {job.workspace} has been archived. Contact the OpenSAFELY tech team to resolve",
                )

        # Check the image exists locally and error if not. Newer versions of
        # docker-cli support `--pull=never` as an argument to `docker run` which
        # would make this simpler, but it looks like it will be a while before this
        # makes it to Docker for Windows:
        # https://github.com/docker/cli/pull/1498
        if not docker.image_exists_locally(job.image):
            log.info(f"Image not found, may need to run: docker pull {job.image}")
            return JobStatus(
                ExecutorState.ERROR,
                f"Docker image {job.image} is not currently available",
            )

        try:
            prepare_job(job)
        except docker.DockerDiskSpaceError as e:
            log.exception(str(e))
            return JobStatus(
                ExecutorState.ERROR, "Out of disk space, please try again later"
            )

        # technically, we're acutally PREPARED, as we did in synchronously, but
        # the loop code is expecting PREPARING, so return that. The next time
        # around the loop, it will pick up that it is PREPARED, and move on.
        return JobStatus(ExecutorState.PREPARING)

    def execute(self, job):
        current = self.get_status(job)
        if current.state != ExecutorState.PREPARED:
            return current

        extra_args = []
        if job.cpu_count:
            extra_args.extend(["--cpus", str(job.cpu_count)])
        if job.memory_limit:
            extra_args.extend(["--memory", job.memory_limit])

        try:
            docker.run(
                container_name(job),
                [job.image] + job.args,
                volume=(volume_name(job), "/workspace"),
                env=job.env,
                allow_network_access=job.allow_database_access,
                label=LABEL,
                labels=get_job_labels(job),
                extra_args=extra_args,
            )
        except Exception as exc:
            return JobStatus(
                ExecutorState.ERROR, f"Failed to start docker container: {exc}"
            )

        return JobStatus(ExecutorState.EXECUTING)

    def finalize(self, job):

        current = self.get_status(job)
        if current.state != ExecutorState.EXECUTED:
            return current

        try:
            finalize_job(job)
        except LocalDockerError as exc:
            return JobStatus(ExecutorState.ERROR, f"failed to finalize job: {exc}")

        return JobStatus(ExecutorState.FINALIZING)

    def terminate(self, job):
        docker.kill(container_name(job))
        return JobStatus(ExecutorState.ERROR, "terminated by api")

    def cleanup(self, job):
        if config.CLEAN_UP_DOCKER_OBJECTS:
            log.info("Cleaning up container and volume")
            docker.delete_container(container_name(job))
            docker.delete_volume(volume_name(job))
        else:
            log.info("Leaving container and volume in place for debugging")

        RESULTS.pop(job.id, None)
        return JobStatus(ExecutorState.UNKNOWN)

    def get_status(self, job):
        name = container_name(job)
        job_running = docker.container_inspect(
            name, "State.Running", none_if_not_exists=True
        )

        if job_running is None:
            # no container for this job found
            volume = volume_name(job)
            if docker.volume_exists(volume):
                return JobStatus(ExecutorState.PREPARED)
            else:
                return JobStatus(ExecutorState.UNKNOWN)

        elif job_running:
            return JobStatus(ExecutorState.EXECUTING)
        elif job.id in RESULTS:
            return JobStatus(ExecutorState.FINALIZED)
        else:  # container present but not running, i.e. finished
            return JobStatus(ExecutorState.EXECUTED)

    def get_results(self, job):
        if job.id not in RESULTS:
            return JobStatus(ExecutorState.ERROR, "job has not been finalized")

        return RESULTS[job.id]

    def delete_files(self, workspace, privacy, files):
        if privacy == Privacy.HIGH:
            root = get_high_privacy_workspace(workspace)
        elif privacy == Privacy.MEDIUM:
            root = get_medium_privacy_workspace(workspace)
        else:
            raise Exception(f"unknown privacy of {privacy}")

        errors = []
        for name in files:
            path = root / name
            try:
                path.unlink(missing_ok=True)
            except Exception:
                log.exception(f"Could not delete {path}")
                errors.append(name)

        return errors


def prepare_job(job):
    """Creates a volume and populates it with the repo and input files."""
    workspace_dir = get_high_privacy_workspace(job.workspace)

    volume = volume_name(job)
    docker.create_volume(volume, get_job_labels(job))

    # `docker cp` can't create parent directories for us so we make sure all
    # these directories get created when we copy in the code
    extra_dirs = set(Path(filename).parent for filename in job.inputs)

    try:
        if job.study.git_repo_url and job.study.commit:
            copy_git_commit_to_volume(
                volume, job.study.git_repo_url, job.study.commit, extra_dirs
            )
        else:
            # We only encounter jobs without a repo or commit when using the
            # "local_run" command to execute uncommitted local code
            copy_local_workspace_to_volume(volume, workspace_dir, extra_dirs)
    except subprocess.CalledProcessError:
        raise LocalDockerError(
            f"Could not checkout commit {job.study.commit} from {job.study.git_repo_url}"
        )

    for filename in job.inputs:
        log.info(f"Copying input file: {filename}")
        if not (workspace_dir / filename).exists():
            raise LocalDockerError(
                f"The file {filename} doesn't exist in workspace {job.workspace} as requested for job {job.id}"
            )
        docker.copy_to_volume(volume, workspace_dir / filename, filename)

    # Hack: see `get_unmatched_outputs`. For some reason this requires a
    # non-empty file so copying `os.devnull` didn't work.
    some_non_empty_file = Path(__file__)
    docker.copy_to_volume(volume, some_non_empty_file, TIMESTAMP_REFERENCE_FILE)
    return volume


def finalize_job(job):

    container_metadata = docker.container_inspect(
        container_name(job), none_if_not_exists=True
    )
    if not container_metadata:
        raise LocalDockerError("Job container has vanished")
    redact_environment_variables(container_metadata)

    outputs, unmatched_patterns = find_matching_outputs(job)
    exit_code = container_metadata["State"]["ExitCode"]

    # First get the user-friendly message for known database exit codes, for jobs
    # that have db access
    message = None

    # special case OOMKilled
    if container_metadata["State"]["OOMKilled"]:
        message = "Ran out of memory"
        memory_limit = container_metadata.get("HostConfig", {}).get("Memory", 0)
        if memory_limit > 0:
            gb_limit = memory_limit / (1024**3)
            message += f" (limit for this job was {gb_limit:.1g}GB)"

    if message is None:
        exit_codes = config.EXIT_CODES.copy()
        if job.allow_database_access:
            # db job exit codes have some more specific meanings
            exit_codes.update(config.DATABASE_EXIT_CODES)
        message = exit_codes.get(exit_code)

    results = JobResults(
        outputs=outputs,
        unmatched_patterns=unmatched_patterns,
        exit_code=container_metadata["State"]["ExitCode"],
        image_id=container_metadata["Image"],
        message=message,
    )
    persist_outputs(job, results.outputs, container_metadata)
    RESULTS[job.id] = results


def persist_outputs(job, outputs, container_metadata):
    """Copy logs and generated outputs to persistant storage."""
    # job_metadata is a big dict capturing everything we know about the state
    # of the job
    job.completed_at = int(time.time())
    job_metadata = dict()
    job_metadata["job_id"] = job.id
    job_metadata["job_request_id"] = job.job_request_id
    job_metadata["created_at"] = job.created_at
    job_metadata["completed_at"] = int(time.time())
    job_metadata["docker_image_id"] = container_metadata["Image"]
    # convert exit code to str so 0 exit codes get logged
    job_metadata["exit_code"] = str(container_metadata["State"]["ExitCode"])
    job_metadata["container_metadata"] = container_metadata
    job_metadata["outputs"] = outputs
    job_metadata["commit"] = job.study.commit

    # Dump useful info in log directory
    log_dir = get_log_dir(job)
    write_log_file(job, job_metadata, log_dir / "logs.txt")
    with open(log_dir / "metadata.json", "w") as f:
        json.dump(job_metadata, f, indent=2)

    # Copy logs to workspace
    workspace_dir = get_high_privacy_workspace(job.workspace)
    metadata_log_file = workspace_dir / METADATA_DIR / f"{job.action}.log"
    copy_file(log_dir / "logs.txt", metadata_log_file)
    log.info(f"Logs written to: {metadata_log_file}")

    # Extract outputs to workspace
    volume = volume_name(job)
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

        # this can be removed once osrelease is dead
        write_manifest_file(
            medium_privacy_dir,
            {"repo": job.study.git_repo_url, "workspace": job.workspace},
        )


def find_matching_outputs(job):
    """
    Returns a dict mapping output filenames to their privacy level, plus a list
    of any patterns that had no matches at all
    """
    all_matches = docker.glob_volume_files(volume_name(job), job.output_spec.keys())
    unmatched_patterns = []
    outputs = {}
    for pattern, privacy_level in job.output_spec.items():
        filenames = all_matches[pattern]
        if not filenames:
            unmatched_patterns.append(pattern)
        for filename in filenames:
            outputs[filename] = privacy_level
    return outputs, unmatched_patterns


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
        f.write("\noutputs:\n")
        f.write(tabulate(outputs, separator="  - ", indent=2, empty="(no outputs)"))


# Keys of fields to log in manifest.json and log file
KEYS_TO_LOG = [
    "job_id",
    "job_request_id",
    "commit",
    "docker_image_id",
    "exit_code",
    "created_at",
    "completed_at",
]


def copy_file(source, dest):
    """Efficient atomic copy.

    shutil.copy uses sendfile on linux, so should be fast.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    with atomic_writer(dest) as tmp:
        shutil.copy(source, tmp)


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
            raise LocalDockerError(
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


def write_manifest_file(workspace_dir, manifest):
    manifest_file = workspace_dir / METADATA_DIR / MANIFEST_FILE
    manifest_file_tmp = manifest_file.with_suffix(".tmp")
    manifest_file_tmp.write_text(json.dumps(manifest, indent=2))
    manifest_file_tmp.replace(manifest_file)
