"""
This module contains the logic for starting jobs in Docker containers and
dealing with them when they are finished.

It's important that the `start_job` and `finalise_job` functions are
idempotent. This means that the job-runner can be killed at any point and will
still end up in a consistent state when it's restarted.
"""
import json
import logging
import shlex
from pathlib import Path
from typing import Tuple

from jobrunner import config, local_docker_job_executor
from jobrunner.job_executor import Privacy, JobAPI, WorkspaceAPI, JobDefinition
from jobrunner.models import State, JobError
from jobrunner.project import (
    is_generate_cohort_command,
)

log = logging.getLogger(__name__)

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


class ActionNotRunError(JobError):
    pass


class ActionFailedError(JobError):
    pass


class MissingOutputError(JobError):
    pass


def load_job_executor() -> Tuple[JobAPI, WorkspaceAPI]:
    return local_docker_job_executor.LocalDockerJobAPI(), local_docker_job_executor.LocalDockerWorkspaceAPI()


jobAPI, workspaceAPI = load_job_executor()


def start_job(job):
    """Start the given job.

    Args:
        job: An instance of Job.
    """
    action_args = shlex.split(job.run_command)
    allow_database_access = False
    env = {"OPENSAFELY_BACKEND": config.BACKEND}
    # Check `is True` so we fail closed if we ever get anything else
    if is_generate_cohort_command(action_args) is True:
        if not config.USING_DUMMY_DATA_BACKEND:
            allow_database_access = True
            env["DATABASE_URL"] = config.DATABASE_URLS[job.database_name]
            if config.TEMP_DATABASE_NAME:
                env["TEMP_DATABASE_NAME"] = config.TEMP_DATABASE_NAME
            if config.PRESTO_TLS_KEY and config.PRESTO_TLS_CERT:
                env["PRESTO_TLS_CERT"] = config.PRESTO_TLS_CERT
                env["PRESTO_TLS_KEY"] = config.PRESTO_TLS_KEY
            if config.EMIS_ORGANISATION_HASH:
                env["EMIS_ORGANISATION_HASH"] = config.EMIS_ORGANISATION_HASH
    # Prepend registry name
    image = action_args.pop(0)
    full_image = f"{config.DOCKER_REGISTRY}/{image}"
    if image.startswith("stata-mp"):
        env["STATA_LICENSE"] = str(config.STATA_LICENSE)

    # Jobs which are running reusable actions pull their code from the reusable
    # action repo, all other jobs pull their code from the study repo
    study = job.action_repo_url or job.repo_url, job.action_commit or job.commit
    # Both of action commit and repo_url should be set if either are
    assert bool(job.action_commit) == bool(job.action_repo_url)

    input_files = {}
    for action in job.requires_outputs_from:
        for filename in list_outputs_from_action(action):
            input_files[filename] = action

    job_def = JobDefinition(job.workspace, study, full_image, action_args, env, input_files, allow_database_access)

    jobAPI.run(job.slug, job_def)


def sync_job_status(job):
    state, results = jobAPI.get_status(job.slug, job.workspace, job.action, job.output_spec)

    if state == State.RUNNING:
        return True
    assert state != State.PENDING

    # Delete outputs from previous run of action. It would be simpler to delete
    # all existing outputs and then copy over the new ones, but this way we
    # don't delete anything until after we've copied the new outputs which is
    # safer in case anything goes wrong.
    existing_files = set(list_outputs_from_action(
        job.action, ignore_errors=True
    ))
    workspaceAPI.delete_files(
        Privacy.HIGH, job.workspace,
        existing_files - set(o for o, privacy in results.outputs if privacy == "highly_sensitive"))
    workspaceAPI.delete_files(
        Privacy.MEDIUM, job.workspace,
        existing_files - set(o for o, privacy in results.outputs if privacy == "moderately_sensitive"))

    job.state = state
    job.status_message = results.status_message
    job.status_code = results.status_code
    job.outputs = results.outputs
    job.unmatched_outputs = results.unmatched_outputs

    # Update manifest
    manifest = read_manifest_file(Path())
    update_manifest(manifest, job, results.outputs)
    write_manifest_file(Path(), manifest)

    return False


def kill_job(job):
    jobAPI.terminate(job.slug)


def get_states_for_actions():
    """
    Return a dictionary mapping action IDs to their current state (if any)
    """
    manifest = read_manifest_file(Path())
    states_by_action = {
        action_id: State(action_details["state"])
        for action_id, action_details in manifest["actions"].items()
    }
    # TODO This is part of the unimplemented call get get workspace state.
    # for filename, file_details in manifest["files"].items():
    #     # If the file has been manually deleted from disk...
    #     if not directory.joinpath(filename).exists():
    #         source_action = file_details["created_by_action"]
    #         # ... remove the action's state as if it hadn't been run
    #         states_by_action.pop(source_action, None)
    return states_by_action


def list_outputs_from_action(action, ignore_errors=False):
    files = {}
    try:
        manifest = read_manifest_file(Path())
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
            # TODO This is part of the unimplemented call get get workspace state.
            # # This would only happen if files were manually deleted from disk
            # if not ignore_errors and not directory.joinpath(filename).exists():
            #     raise MissingOutputError(f"Output {filename} missing from {action}")
    return output_files


# TODO All manifest handling to be replaced by data stored in SQLite.
def read_manifest_file(workspace_dir):
    """
    Read the manifest of a given workspace, returning an empty manifest if none
    found
    """
    try:
        with open(workspace_dir / "something" / MANIFEST_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {"files": {}, "actions": {}}


def update_manifest(manifest, job, new_outputs):
    action = job.action
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
    manifest["workspace"] = job.workspace
    manifest["repo"] = job.repo_url
    manifest["files"] = dict(files)
    # Popping and re-adding means the action gets moved to the end of the dict
    # so actions end up in the order they were run
    manifest["actions"].pop(action, None)
    manifest["actions"][action] = {
        # TODO Some of these keys are not be available here, outside the job-executor.
        key: getattr(job, key) for key in KEYS_TO_LOG if hasattr(job, key)
    }


def write_manifest_file(workspace_dir, manifest):
    manifest_file = workspace_dir / "something" / MANIFEST_FILE
    manifest_file_tmp = manifest_file.with_suffix(".tmp")
    manifest_file_tmp.write_text(json.dumps(manifest, indent=2))
    manifest_file_tmp.replace(manifest_file)
