import json
import logging
import subprocess
from pathlib import Path

from jobrunner.job_executor import (
    ExecutorState,
    JobDefinition,
    JobResults,
    JobStatus,
    Privacy,
)
from jobrunner.lib import docker
from jobrunner.lib.string_utils import tabulate
# ideally, these should be moved into this module when the old implementation
# is removed
from jobrunner.manage_jobs import (
    METADATA_DIR,
    TIMESTAMP_REFERENCE_FILE,
    cleanup_job,
    container_name,
    copy_file,
    copy_git_commit_to_volume,
    copy_local_workspace_to_volume,
    ensure_overwritable,
    get_container_metadata,
    get_high_privacy_workspace,
    get_log_dir,
    get_medium_privacy_workspace,
    volume_name,
    write_manifest_file,
)

# cache of result objects
RESULTS = {}
LABEL = "jobrunner-local"

log = logging.getLogger(__name__)


class LocalDockerError(Exception):
    pass


class LocalDockerAPI:
    """ExecutorAPI implementation using local docker service."""

    def prepare(self, job):
        current = self.get_status(job)
        if current.state != ExecutorState.UNKNOWN:
            return current

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

        try:
            docker.run(
                container_name(job),
                [job.image] + job.args,
                volume=(volume_name(job), "/workspace"),
                env=job.env,
                allow_network_access=job.allow_database_access,
                label=LABEL,
                labels={
                    # make it easier to find stuff
                    "workspace": job.workspace,
                    "action": job.action,
                },
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
        cleanup_job(job)
        RESULTS.pop(job.id, None)

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
    docker.create_volume(volume)

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
    container_metadata = get_container_metadata(job)
    outputs, unmatched_patterns = find_matching_outputs(job)
    results = JobResults(
        outputs,
        unmatched_patterns,
        container_metadata["State"]["ExitCode"],
        container_metadata["Image"],
    )
    persist_outputs(job, results.outputs, container_metadata)
    RESULTS[job.id] = results


def persist_outputs(job, outputs, container_metadata):
    """Copy logs and generated outputs to persistant storage."""
    # job_metadata is a big dict capturing everything we know about the state
    # of the job
    job_metadata = dict()
    job_metadata["id"] = job.id
    job_metadata["docker_image_id"] = container_metadata["Image"]
    job_metadata["exit_code"] = container_metadata["State"]["ExitCode"]
    job_metadata["container_metadata"] = container_metadata
    job_metadata["outputs"] = outputs
    job_metadata["commit"] = job.study.commit

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
    ensure_overwritable(*[workspace_dir / f for f in outputs.keys()])
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
    "id",
    "commit",
    "docker_image_id",
    "exit_code",
]
