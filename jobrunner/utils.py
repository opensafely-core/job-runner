import glob
import logging
import os
import posixpath
import re
import subprocess
from pathlib import Path

NEW_JOB_KEYS = [
    "force_run",
    "force_run_dependencies",
    "action_id",
    "backend",
    "needed_by_id",
    "workspace_id",
]


def writable_job_subset(job):
    new_job = {}
    for k in NEW_JOB_KEYS:
        new_job[k] = job.get(k, None)
    return new_job


def getlogger(name):
    """Create a custom logger with a field for recording a unique job id
    """
    FORMAT = "%(asctime)-15s %(levelname)-10s  %(job_id)-10s %(message)s"
    formatter = logging.Formatter(FORMAT)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def get_auth():
    return (os.environ["QUEUE_USER"], os.environ["QUEUE_PASS"])


def get_workdir():
    return "/workspace"


def safe_join(startdir, path):
    """Given a `startdir` and `path`, join them together, while protecting against directory traversal attacks that take us outside `startdir`
    """
    requested_path = posixpath.normpath(posixpath.join(startdir, *path.split("\\")))
    startdir = str(startdir)  # Normalise from PosixPath
    assert (
        posixpath.commonprefix([requested_path, startdir]) == startdir
    ), f"Invalid requested path {requested_path}, not in {startdir}"
    return requested_path


def make_volume_name(action):
    """Create a string suitable for naming a folder that will contain
    data, using state related to the current workspace as a unique key.

    """
    parts = [action["backend"]]
    if action.get("run_locally"):
        keys = ["branch", "db", "name"]
    else:
        keys = ["repo", "branch", "db", "name"]
    for key in keys:
        # Remove symbols (excluding hyphens)
        parts.append(re.sub(r"[^0-9a-z-]", "-", action["workspace"][key]))
    # Dedupe hyphens
    parts = "-".join(parts)
    parts = re.sub(r"--+", "-", parts)
    return parts


def make_output_bucket(action, privacy_level):
    volume_name = make_volume_name(action)
    if privacy_level == "highly_sensitive":
        storage_base = Path(os.environ["HIGH_PRIVACY_STORAGE_BASE"])
    elif privacy_level == "moderately_sensitive":
        storage_base = Path(os.environ["MEDIUM_PRIVACY_STORAGE_BASE"])
    output_bucket = storage_base / volume_name
    output_bucket.mkdir(parents=True, exist_ok=True)
    return str(output_bucket)


def all_output_paths_for_action(action):
    """Given an action, provide a dictionary showing location for each of its outputs

    """
    paths = []
    for privacy_level, outputs in action.get("outputs", {}).items():
        for output_name, relative_path in outputs.items():
            namespace = safe_join(action["action_id"], output_name)
            paths.append(
                {
                    "base_path": make_output_bucket(action, privacy_level),
                    "namespace": namespace,
                    "relative_path": relative_path,
                }
            )
    return paths


def needs_run(action):
    """Flag if a job should be run, either because it's been explicitly
    requested, or because any of its output files are missing.

    In the case of a globbed output, the existence of *any* file matching the
    glob counts as a successful run that doesn't need re-running.
    """
    if action["force_run"]:
        return True
    for output in action["output_locations"]:
        namespaced_path = safe_join(output["namespace"], output["relative_path"])
        full_path = safe_join(output["base_path"], namespaced_path)
        existing_outputs = glob.glob(full_path)
        if not existing_outputs:
            return True
    return False


def docker_container_exists(container_name):
    cmd = [
        "docker",
        "ps",
        "--filter",
        f"name={container_name}",
        "--quiet",
    ]
    result = subprocess.run(cmd, capture_output=True, encoding="utf8")
    return result.stdout != ""
