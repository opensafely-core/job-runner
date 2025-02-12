import csv
import datetime
import json
import logging
import socket
import subprocess
import tempfile
import time
import urllib.parse
from pathlib import Path

from pipeline.legacy import get_all_output_patterns_from_project_file

from jobrunner import config, record_stats
from jobrunner.executors import volumes
from jobrunner.job_executor import (
    ExecutorAPI,
    ExecutorRetry,
    ExecutorState,
    JobDefinition,
    JobResults,
    JobStatus,
    Privacy,
)
from jobrunner.lib import datestr_to_ns_timestamp, docker, file_digest
from jobrunner.lib.git import checkout_commit
from jobrunner.lib.path_utils import list_dir_with_ignore_patterns
from jobrunner.lib.string_utils import tabulate


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


def container_name(job_definition):
    return f"os-job-{job_definition.id}"


def get_high_privacy_workspace(workspace):
    return config.HIGH_PRIVACY_WORKSPACES_DIR / workspace


def get_medium_privacy_workspace(workspace):
    if config.MEDIUM_PRIVACY_WORKSPACES_DIR:
        return config.MEDIUM_PRIVACY_WORKSPACES_DIR / workspace
    else:
        return None


def get_log_dir(job_definition):
    # Split log directory up by month to make things slightly more manageable
    month_dir = datetime.date.today().strftime("%Y-%m")
    return config.JOB_LOG_DIR / month_dir / container_name(job_definition)


class LocalDockerError(Exception):
    pass


def get_job_labels(job_definition: JobDefinition):
    """Useful metadata to label docker objects with."""
    return {
        "workspace": job_definition.workspace,
        "action": job_definition.action,
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

    synchronous_transitions = [ExecutorState.PREPARING, ExecutorState.FINALIZING]

    def prepare(self, job_definition):
        # Check the workspace is not archived
        workspace_dir = get_high_privacy_workspace(job_definition.workspace)
        if not workspace_dir.exists():
            if workspace_is_archived(job_definition.workspace):
                return JobStatus(
                    ExecutorState.ERROR,
                    f"Workspace {job_definition.workspace} has been archived. Contact the OpenSAFELY tech team to resolve",
                )

        # Check the image exists locally and error if not. Newer versions of
        # docker-cli support `--pull=never` as an argument to `docker run` which
        # would make this simpler, but it looks like it will be a while before this
        # makes it to Docker for Windows:
        # https://github.com/docker/cli/pull/1498
        if not docker.image_exists_locally(job_definition.image):
            log.info(
                f"Image not found, may need to run: docker pull {job_definition.image}"
            )
            return JobStatus(
                ExecutorState.ERROR,
                f"Docker image {job_definition.image} is not currently available",
            )

        current = self.get_status(job_definition)
        if current.state != ExecutorState.UNKNOWN:
            return current

        try:
            prepare_job(job_definition)
        except docker.DockerDiskSpaceError as e:
            log.exception(str(e))
            return JobStatus(
                ExecutorState.ERROR, "Out of disk space, please try again later"
            )

        # this API is synchronous, so we are PREPARED now
        return JobStatus(ExecutorState.PREPARED)

    def execute(self, job_definition):
        current = self.get_status(job_definition)
        if current.state != ExecutorState.PREPARED:
            return current
        volume_api = volumes.get_volume_api(job_definition)

        extra_args = []
        if job_definition.cpu_count:
            extra_args.extend(["--cpus", str(job_definition.cpu_count)])
        if job_definition.memory_limit:
            extra_args.extend(["--memory", job_definition.memory_limit])
        # We use a custom Docker network configured so that database jobs can access the
        # database and nothing else
        if job_definition.allow_database_access and config.DATABASE_ACCESS_NETWORK:
            extra_args.extend(["--network", config.DATABASE_ACCESS_NETWORK])
            extra_args.extend(
                get_dns_args_for_docker(job_definition.env.get("DATABASE_URL"))
            )

        if not volume_api.requires_root:
            if config.DOCKER_USER_ID and config.DOCKER_GROUP_ID:
                extra_args.extend(
                    [
                        "--user",
                        f"{config.DOCKER_USER_ID}:{config.DOCKER_GROUP_ID}",
                    ]
                )
            extra_args.extend(
                [
                    "-e",
                    "HOME=/tmp",  # set home dir to something writable by non-root user
                ]
            )

        try:
            docker.run(
                container_name(job_definition),
                [job_definition.image] + job_definition.args,
                volume=(volume_api.volume_name(job_definition), "/workspace"),
                env=job_definition.env,
                allow_network_access=job_definition.allow_database_access,
                label=LABEL,
                labels=get_job_labels(job_definition),
                extra_args=extra_args,
                volume_type=volume_api.volume_type,
            )

        except Exception as exc:
            return JobStatus(
                ExecutorState.ERROR, f"Failed to start docker container: {exc}"
            )

        return JobStatus(ExecutorState.EXECUTING)

    def finalize(self, job_definition):
        current_status = self.get_status(job_definition)
        if current_status.state == ExecutorState.UNKNOWN:
            # job had not started running, so do not finalize
            return current_status

        assert current_status.state in [ExecutorState.EXECUTED, ExecutorState.ERROR]

        try:
            finalize_job(job_definition)
        except LocalDockerError as exc:
            return JobStatus(ExecutorState.ERROR, f"failed to finalize job: {exc}")

        # this api is synchronous, so we are now FINALIZED
        return JobStatus(ExecutorState.FINALIZED)

    def terminate(self, job_definition):
        current_status = self.get_status(job_definition)
        if current_status.state == ExecutorState.UNKNOWN:
            # job was pending, so do not go to EXECUTED
            return current_status

        if current_status.state in [
            ExecutorState.EXECUTED,
            ExecutorState.FINALIZED,
            ExecutorState.FINALIZING,
        ]:
            # job has already finished - whilst this function should not be called in
            # this case, it's possible it could happen due to a race condition
            return current_status

        assert current_status.state in [
            ExecutorState.EXECUTING,
            ExecutorState.ERROR,
            ExecutorState.PREPARED,
        ], f"unexpected status {current_status}"

        docker.kill(container_name(job_definition))

        return JobStatus(
            ExecutorState.EXECUTED, f"Job terminated by {job_definition.cancelled}"
        )

    def cleanup(self, job_definition):
        if config.CLEAN_UP_DOCKER_OBJECTS:
            log.info("Cleaning up container and volume")
            docker.delete_container(container_name(job_definition))
            volumes.get_volume_api(job_definition).delete_volume(job_definition)
        else:
            log.info("Leaving container and volume in place for debugging")

        RESULTS.pop(job_definition.id, None)
        return JobStatus(ExecutorState.UNKNOWN)

    def get_status(self, job_definition, timeout=15):
        name = container_name(job_definition)
        try:
            container = docker.container_inspect(
                name,
                none_if_not_exists=True,
                timeout=timeout,
            )
        except docker.DockerTimeoutError:
            raise ExecutorRetry(
                f"docker timed out after {timeout}s inspecting container {name}"
            )

        metrics = record_stats.read_job_metrics(job_definition.id)

        if container is None:  # container doesn't exist
            if job_definition.cancelled:
                if volumes.get_volume_api(job_definition).volume_exists(job_definition):
                    # jobs prepared but not running do not need to finalize, so we
                    # proceed directly to the FINALIZED state here
                    return JobStatus(
                        ExecutorState.FINALIZED,
                        "Prepared job was cancelled",
                        metrics=metrics,
                    )
                else:
                    return JobStatus(
                        ExecutorState.UNKNOWN,
                        "Pending job was cancelled",
                        metrics=metrics,
                    )

            # timestamp file presence means we have finished preparing
            timestamp_ns = volumes.get_volume_api(job_definition).read_timestamp(
                job_definition, TIMESTAMP_REFERENCE_FILE, 10
            )
            # TODO: maybe log the case where the volume exists, but the
            # timestamp file does not? It's not a problems as the loop should
            # re-prepare it anyway.
            if timestamp_ns is None:
                # we are Jon Snow
                return JobStatus(ExecutorState.UNKNOWN, metrics={})
            else:
                # we've finish preparing
                return JobStatus(
                    ExecutorState.PREPARED, timestamp_ns=timestamp_ns, metrics=metrics
                )

        if container["State"]["Running"]:
            timestamp_ns = datestr_to_ns_timestamp(container["State"]["StartedAt"])
            return JobStatus(
                ExecutorState.EXECUTING, timestamp_ns=timestamp_ns, metrics=metrics
            )
        elif job_definition.id in RESULTS:
            return JobStatus(
                ExecutorState.FINALIZED,
                timestamp_ns=RESULTS[job_definition.id].timestamp_ns,
                metrics=metrics,
            )
        else:
            # container present but not running, i.e. finished
            # Nb. this does not include prepared jobs, as they have a volume but not a container
            timestamp_ns = datestr_to_ns_timestamp(container["State"]["FinishedAt"])
            return JobStatus(
                ExecutorState.EXECUTED, timestamp_ns=timestamp_ns, metrics=metrics
            )

    def get_results(self, job_definition):
        if job_definition.id not in RESULTS:
            return JobStatus(ExecutorState.ERROR, "job has not been finalized")

        return RESULTS[job_definition.id]

    def delete_files(self, workspace, privacy, files):
        if privacy == Privacy.HIGH:
            root = get_high_privacy_workspace(workspace)
        elif privacy == Privacy.MEDIUM:
            root = get_medium_privacy_workspace(workspace)
        else:
            raise Exception(f"unknown privacy of {privacy}")

        return delete_files_from_directory(root, files)


def delete_files_from_directory(directory, files):
    errors = []
    for name in files:
        path = directory / name
        try:
            path.unlink(missing_ok=True)
        except Exception:
            log.exception(f"Could not delete {path}")
            errors.append(name)

    return errors


def prepare_job(job_definition):
    """Creates a volume and populates it with the repo and input files."""
    workspace_dir = get_high_privacy_workspace(job_definition.workspace)

    volume_api = volumes.get_volume_api(job_definition)
    volume_api.create_volume(job_definition, get_job_labels(job_definition))

    # `docker cp` can't create parent directories for us so we make sure all
    # these directories get created when we copy in the code
    extra_dirs = set(Path(filename).parent for filename in job_definition.inputs)

    try:
        if job_definition.study.git_repo_url and job_definition.study.commit:
            copy_git_commit_to_volume(
                job_definition,
                job_definition.study.git_repo_url,
                job_definition.study.commit,
                extra_dirs,
            )
        else:
            # We only encounter jobs without a repo or commit when using the
            # "local_run" command to execute uncommitted local code
            copy_local_workspace_to_volume(job_definition, workspace_dir, extra_dirs)
    except subprocess.CalledProcessError:
        raise LocalDockerError(
            f"Could not checkout commit {job_definition.study.commit} from {job_definition.study.git_repo_url}"
        )

    for filename in job_definition.inputs:
        log.info(f"Copying input file: {filename}")
        if not (workspace_dir / filename).exists():
            raise LocalDockerError(
                f"The file {filename} doesn't exist in workspace {job_definition.workspace} as requested for job {job_definition.id}"
            )
        volume_api.copy_to_volume(job_definition, workspace_dir / filename, filename)

    # Used to record state for telemetry, and also see `get_unmatched_outputs`
    volume_api.write_timestamp(job_definition, TIMESTAMP_REFERENCE_FILE)


def finalize_job(job_definition):
    container_metadata = docker.container_inspect(
        container_name(job_definition), none_if_not_exists=True
    )
    if not container_metadata:
        if job_definition.cancelled:
            # no logs to retain if the container didn't start yet
            return
        else:
            raise LocalDockerError(
                f"Job container {container_name(job_definition)} has vanished"
            )
    redact_environment_variables(container_metadata)

    if job_definition.cancelled:
        # assume no outputs because our job didn't finish
        outputs = {}
        unmatched_patterns = []
        unmatched_outputs = []
    else:
        outputs, unmatched_patterns = find_matching_outputs(job_definition)
        unmatched_outputs = get_unmatched_outputs(job_definition, outputs)

    exit_code = container_metadata["State"]["ExitCode"]
    labels = container_metadata.get("Config", {}).get("Labels", {})

    # First get the user-friendly message for known database exit codes, for jobs
    # that have db access
    unmatched_hint = None

    if exit_code == 0 and outputs and not unmatched_patterns:
        message = "Completed successfully"

    elif exit_code == 0 and unmatched_patterns:
        message = "\n  No outputs found matching patterns:\n - {}".format(
            "\n   - ".join(unmatched_patterns)
        )
        if unmatched_outputs:
            unmatched_hint = (
                "\n  Did you mean to match one of these files instead?\n - {}".format(
                    "\n   - ".join(unmatched_outputs)
                )
            )

    elif exit_code == 137 and job_definition.cancelled:
        message = f"Job cancelled by {job_definition.cancelled}"
    # Nb. this flag has been observed to be unreliable on some versions of Linux
    elif (
        container_metadata["State"]["ExitCode"] == 137
        and container_metadata["State"]["OOMKilled"]
    ):
        message = "Job ran out of memory"
        memory_limit = container_metadata.get("HostConfig", {}).get("Memory", 0)
        if memory_limit > 0:
            gb_limit = memory_limit / (1024**3)
            message += f" (limit was {gb_limit:.2f}GB)"
    else:
        message = config.DOCKER_EXIT_CODES.get(exit_code)

    results = JobResults(
        outputs=outputs,
        unmatched_patterns=unmatched_patterns,
        unmatched_outputs=unmatched_outputs,
        exit_code=exit_code,
        image_id=container_metadata["Image"],
        message=message,
        unmatched_hint=unmatched_hint,
        timestamp_ns=time.time_ns(),
        action_version=labels.get("org.opencontainers.image.version", "unknown"),
        action_revision=labels.get("org.opencontainers.image.revision", "unknown"),
        action_created=labels.get("org.opencontainers.image.created", "unknown"),
        base_revision=labels.get("org.opensafely.base.vcs-ref", "unknown"),
        base_created=labels.get("org.opencontainers.base.build-date", "unknown"),
    )
    job_metadata = get_job_metadata(
        job_definition, outputs, container_metadata, results
    )

    if job_definition.cancelled:
        write_job_logs(job_definition, job_metadata, copy_log_to_workspace=False)
    else:
        excluded = persist_outputs(job_definition, results.outputs, job_metadata)
        write_job_logs(
            job_definition, job_metadata, copy_log_to_workspace=True, excluded=excluded
        )
        results.level4_excluded_files.update(**excluded)

    RESULTS[job_definition.id] = results

    # for ease of testing
    return results


def get_job_metadata(job_definition, outputs, container_metadata, results):
    # job_metadata is a big dict capturing everything we know about the state
    # of the job
    job_metadata = dict()
    job_metadata["job_definition_id"] = job_definition.id
    job_metadata["job_definition_request_id"] = job_definition.job_request_id
    job_metadata["created_at"] = job_definition.created_at
    job_metadata["completed_at"] = int(time.time())
    job_metadata["docker_image_id"] = container_metadata["Image"]
    # convert exit code to str so 0 exit codes get logged
    job_metadata["exit_code"] = str(container_metadata["State"]["ExitCode"])
    job_metadata["status_message"] = results.message
    job_metadata["container_metadata"] = container_metadata
    job_metadata["outputs"] = outputs
    job_metadata["commit"] = job_definition.study.commit
    job_metadata["database_name"] = job_definition.database_name
    job_metadata["hint"] = results.unmatched_hint
    return job_metadata


def write_job_logs(
    job_definition, job_metadata, copy_log_to_workspace=True, excluded=None
):
    """Copy logs to log dir and workspace."""
    # Dump useful info in log directory
    log_dir = get_log_dir(job_definition)
    write_log_file(job_definition, job_metadata, log_dir / "logs.txt", excluded)
    with open(log_dir / "metadata.json", "w") as f:
        json.dump(job_metadata, f, indent=2)

    if copy_log_to_workspace:
        workspace_dir = get_high_privacy_workspace(job_definition.workspace)
        workspace_log_file = (
            workspace_dir / METADATA_DIR / f"{job_definition.action}.log"
        )
        volumes.copy_file(log_dir / "logs.txt", workspace_log_file)
        log.info(f"Logs written to: {workspace_log_file}")

        medium_privacy_dir = get_medium_privacy_workspace(job_definition.workspace)
        if medium_privacy_dir:
            volumes.copy_file(
                workspace_log_file,
                medium_privacy_dir / METADATA_DIR / f"{job_definition.action}.log",
            )


def persist_outputs(job_definition, outputs, job_metadata):
    """Copy generated outputs to persistant storage."""
    # Extract outputs to workspace
    workspace_dir = get_high_privacy_workspace(job_definition.workspace)

    excluded_job_msgs = {}
    excluded_file_msgs = {}

    sizes = {}
    # copy all files into workspace long term storage
    for filename, level in outputs.items():
        log.info(f"Extracting output file: {filename}")
        dst = workspace_dir / filename
        sizes[filename] = volumes.get_volume_api(job_definition).copy_from_volume(
            job_definition, filename, dst
        )

    l4_files = [
        filename
        for filename, level in outputs.items()
        if level == "moderately_sensitive"
    ]

    csv_metadata = {}
    # check any L4 files are vaild
    for filename in l4_files:
        ok, job_msg, file_msg, csv_counts = check_l4_file(
            job_definition, filename, sizes[filename], workspace_dir
        )
        if not ok:
            excluded_job_msgs[filename] = job_msg
            excluded_file_msgs[filename] = file_msg
        csv_metadata[filename] = csv_counts
    medium_privacy_dir = get_medium_privacy_workspace(job_definition.workspace)

    # local run currently does not have a level 4 directory, so exit early
    if not medium_privacy_dir:
        return excluded_job_msgs

    # Copy out medium privacy files to L4
    for filename in l4_files:
        src = workspace_dir / filename
        dst = medium_privacy_dir / filename
        message_file = medium_privacy_dir / (filename + ".txt")

        if filename in excluded_file_msgs:
            message_file.parent.mkdir(exist_ok=True, parents=True)
            message_file.write_text(excluded_file_msgs[filename])
        else:
            volumes.copy_file(src, dst)
            # if it previously had a message, delete it
            delete_files_from_directory(medium_privacy_dir, [message_file])

    new_outputs = {}

    for filename, level in outputs.items():
        abspath = workspace_dir / filename
        new_outputs[filename] = get_output_metadata(
            abspath,
            level,
            job_id=job_definition.id,
            job_request=job_definition.job_request_id,
            action=job_definition.action,
            commit=job_definition.study.commit,
            repo=job_definition.study.git_repo_url,
            excluded=filename in excluded_file_msgs,
            message=excluded_job_msgs.get(filename),
            csv_counts=csv_metadata.get(filename),
        )

    # Update manifest with file metdata
    manifest = read_manifest_file(medium_privacy_dir, job_definition.workspace)
    manifest["outputs"].update(**new_outputs)
    write_manifest_file(medium_privacy_dir, manifest)

    return excluded_job_msgs


def get_output_metadata(
    abspath,
    level,
    job_id,
    job_request,
    action,
    commit,
    repo,
    excluded,
    message=None,
    csv_counts=None,
):
    stat = abspath.stat()
    with abspath.open("rb") as fp:
        content_hash = file_digest(fp, "sha256").hexdigest()
    csv_counts = csv_counts or {}
    return {
        "level": level,
        "job_id": job_id,
        "job_request": job_request,
        "action": action,
        "repo": repo,
        "commit": commit,
        "size": stat.st_size,
        "timestamp": stat.st_mtime,
        "content_hash": content_hash,
        "excluded": excluded,
        "message": message,
        "row_count": csv_counts.get("rows"),
        "col_count": csv_counts.get("cols"),
    }


MAX_SIZE_MSG = """
The file:

{filename}

was {size}Mb, which is above the limit for moderately_sensitive files of
{limit}Mb.

As such, it has *not* been copied to Level 4 storage. Please double check that
{filename} contains only aggregate information, and is an appropriate size to
be able to be output checked.
"""

INVALID_FILE_TYPE_MSG = """
The file:

{filename}

is of type {suffix}. This is not a valid file type for moderately_sensitive files.

Level 4 files should be aggregate information easily viewable by output checkers.

See available list of file types here: https://docs.opensafely.org/requesting-file-release/#allowed-file-types.
"""

PATIENT_ID = """
The file:

{filename}

has not been made available in level 4 because it has a `patient_id` column.

Patient level data is not allowed by policy in level 4.

You should change this file's privacy to `highly_sensitive` in your
project.yaml. Or, if is aggregrate data, you should remove the patient_id
column from your data.

"""

MAX_CSV_ROWS_MSG = """
The file:

{filename}

contained {row_count} rows, which is above the limit for moderately_sensitive files of
{limit} rows.

As such, it has *not* been copied to Level 4 storage. Please contact tech-support for
further assistance.
"""


def get_csv_counts(path):
    csv_counts = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        first_row = next(reader, None)
        if first_row:
            csv_counts["cols"] = len(first_row)
            csv_counts["rows"] = sum(1 for _ in reader) + 1
        else:
            csv_counts["cols"] = csv_counts["rows"] = 0

    return csv_counts, headers


def check_l4_file(job_definition, filename, size, workspace_dir):
    def mb(b):
        return round(b / (1024 * 1024), 2)

    job_msgs = []
    file_msgs = []
    csv_counts = {"rows": None, "cols": None}
    headers = []

    suffix = Path(filename).suffix

    if size > job_definition.level4_max_filesize:
        job_msgs.append(
            f"File size of {mb(size)}Mb is larger that limit of {mb(job_definition.level4_max_filesize)}Mb."
        )
        file_msgs.append(
            MAX_SIZE_MSG.format(
                filename=filename,
                size=mb(size),
                limit=mb(job_definition.level4_max_filesize),
            )
        )
    elif suffix not in config.LEVEL4_FILE_TYPES:
        job_msgs.append(f"File type of {suffix} is not valid level 4 file")
        file_msgs.append(INVALID_FILE_TYPE_MSG.format(filename=filename, suffix=suffix))

    elif suffix == ".csv":
        # note: this assumes the local executor can directly access the long term storage on disk
        # this may need to be abstracted in future
        actual_file = workspace_dir / filename
        try:
            csv_counts, headers = get_csv_counts(actual_file)
        except Exception:
            pass
        else:
            if headers and "patient_id" in headers:
                job_msgs.append("File has patient_id column")
                file_msgs.append(PATIENT_ID.format(filename=filename))
            if csv_counts["rows"] > job_definition.level4_max_csv_rows:
                job_msgs.append(
                    f"File row count ({csv_counts['rows']}) exceeds maximum allowed rows ({job_definition.level4_max_csv_rows})"
                )
                file_msgs.append(
                    MAX_CSV_ROWS_MSG.format(
                        filename=filename,
                        row_count=csv_counts["rows"],
                        limit=job_definition.level4_max_csv_rows,
                    )
                )

    if job_msgs:
        return False, ",".join(job_msgs), "\n\n".join(file_msgs), csv_counts
    else:
        return True, None, None, csv_counts


def find_matching_outputs(job_definition):
    """
    Returns a dict mapping output filenames to their privacy level, plus a list
    of any patterns that had no matches at all
    """
    all_matches = volumes.get_volume_api(job_definition).glob_volume_files(
        job_definition
    )
    unmatched_patterns = []
    outputs = {}
    for pattern, privacy_level in job_definition.output_spec.items():
        filenames = all_matches[pattern]
        if not filenames:
            unmatched_patterns.append(pattern)
        for filename in filenames:
            outputs[filename] = privacy_level
    return outputs, unmatched_patterns


def get_unmatched_outputs(job_definition, outputs):
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
    all_outputs = volumes.get_volume_api(job_definition).find_newer_files(
        job_definition, TIMESTAMP_REFERENCE_FILE
    )
    return [filename for filename in all_outputs if filename not in outputs]


def write_log_file(job_definition, job_metadata, filename, excluded):
    """
    This dumps the (timestamped) Docker logs for a job to disk, followed by
    some useful metadata about the job and its outputs
    """
    filename.parent.mkdir(parents=True, exist_ok=True)
    docker.write_logs_to_file(container_name(job_definition), filename)
    outputs = sorted(job_metadata["outputs"].items())
    with open(filename, "a") as f:
        f.write("\n\n")
        for key in KEYS_TO_LOG:
            if not job_metadata[key]:
                continue
            f.write(f"{key}: {job_metadata[key]}\n")
        f.write("\noutputs:\n")
        f.write(tabulate(outputs, separator="  - ", indent=2, empty="(no outputs)"))
        if excluded:
            f.write("\n")
            f.write("\nInvalid moderately_sensitive outputs:\n")
            f.write(tabulate(excluded.items(), separator="  - ", indent=2))
        f.write("\n")


# Keys of fields to log in manifest.json and log file
KEYS_TO_LOG = [
    "job_definition_id",
    "job_definition_request_id",
    "commit",
    "docker_image_id",
    "exit_code",
    "created_at",
    "completed_at",
    "database_name",
    "status_message",
    "hint",
]


def copy_git_commit_to_volume(job_definition, repo_url, commit, extra_dirs):
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
            volumes.get_volume_api(job_definition).copy_to_volume(
                job_definition, tmpdir, ".", timeout=60
            )
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


def copy_local_workspace_to_volume(job_definition, workspace_dir, extra_dirs):
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
    volume_api = volumes.get_volume_api(job_definition)
    if directories:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            for directory in directories:
                tmpdir.joinpath(directory).mkdir(parents=True, exist_ok=True)
            volume_api.copy_to_volume(job_definition, tmpdir, ".")

    log.info(f"Copying in code from {workspace_dir}")
    for filename in code_files:
        volumes.get_volume_api(job_definition).copy_to_volume(
            job_definition, workspace_dir / filename, filename
        )


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
        (
            f"{key}=xxxx-REDACTED-xxxx"
            if key not in SAFE_ENVIRONMENT_VARIABLES
            else f"{key}={value}"
        )
        for (key, value) in env_vars
    ]
    container_metadata["Config"]["Env"] = redacted_vars


def read_manifest_file(workspace_dir, workspace):
    manifest_file = workspace_dir / METADATA_DIR / MANIFEST_FILE

    if manifest_file.exists():
        manifest = json.loads(manifest_file.read_text())
        manifest.setdefault("outputs", {})
        return manifest

    return {
        "workspace": workspace,
        "repo": None,  # old key, no longer needed
        "outputs": {},
    }


def write_manifest_file(workspace_dir, manifest):
    manifest_file = workspace_dir / METADATA_DIR / MANIFEST_FILE
    manifest_file.parent.mkdir(exist_ok=True, parents=True)
    manifest_file_tmp = manifest_file.with_suffix(".tmp")
    manifest_file_tmp.write_text(json.dumps(manifest, indent=2))
    manifest_file_tmp.replace(manifest_file)


def get_dns_args_for_docker(database_url):
    # This is various shades of horrible. For containers on a custom network, Docker
    # creates an embedded DNS server, available on 127.0.0.11 from within the container.
    # This proxies non-local requests out to the host DNS server. We want to lock these
    # containers down the absolute bare minimum of network access, which does not
    # include DNS. However there is no way of disabling this embedded server, see:
    # https://github.com/moby/moby/issues/19474
    #
    # As a workaround, we give it a "dummy" IP in place of the host resolver so that
    # requests from inside the container never go anywhere. This IP was taken from the
    # reserved test range specified in:
    # https://www.rfc-editor.org/rfc/rfc5737
    args = ["--dns", "192.0.2.0"]

    # Where the database URL uses a hostname rather than an IP, we resolve that here and
    # use the `--add-host` option to include it in the container's `/etc/hosts` file.
    if database_url:
        database_host = urllib.parse.urlparse(database_url).hostname
        database_ip = socket.gethostbyname(database_host)
        if database_host != database_ip:
            args.extend(["--add-host", f"{database_host}:{database_ip}"])
    return args
