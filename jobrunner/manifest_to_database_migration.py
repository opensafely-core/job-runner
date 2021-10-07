import json
import sys

from jobrunner import config
from jobrunner.lib import database
from jobrunner.manage_jobs import MANIFEST_FILE, METADATA_DIR
from jobrunner.models import Job, State, isoformat_to_timestamp


def migrate_all(batch_size=10):
    _migrate(config.HIGH_PRIVACY_WORKSPACES_DIR.iterdir(), batch_size)


def migrate_one(workspace_dir, batch_size=10):
    _migrate([workspace_dir], batch_size)


def _migrate(workspace_dirs, batch_size):
    count = 0

    for job in _jobs_from_workspaces(workspace_dirs):
        if count >= batch_size:
            _log(
                f"Reached batch size of {batch_size}. There are more jobs to be migrated."
            )
            break

        if database.exists_where(Job, id=job.id):
            continue

        database.insert(job)
        _log(f"Inserted Job(id={job.id}, action={job.action}).")
        count += 1

    if count == 0:
        _log("There were no jobs to migrate.")


def _jobs_from_workspaces(workspace_dirs):
    for workspace_dir in workspace_dirs:
        yield from _jobs_from_workspace(workspace_dir)


def _jobs_from_workspace(workspace_dir):
    manifest_file = workspace_dir / METADATA_DIR / MANIFEST_FILE
    if not manifest_file.exists():
        return

    manifest = json.load(manifest_file.open())
    workspace_name = manifest["workspace"]
    all_files = manifest.get("files", {}).items()
    _log(f"Migrating workspace {workspace_name} in directory {workspace_dir.name}.")

    for action, action_details in manifest["actions"].items():
        files = {
            file: file_details
            for file, file_details in all_files
            if file_details["created_by_action"] == action
        }
        yield _action_to_job(
            workspace_name, manifest.get("repo"), files, action, action_details
        )


def _action_to_job(workspace, repo, files, action, details):
    return Job(
        id=details["job_id"],
        workspace=workspace,
        repo_url=repo,
        action=action,
        state=_map_get(details, "state", State.__getitem__, None),
        commit=details.get("commit"),
        image_id=details.get("docker_image_id"),
        created_at=_map_get(details, "created_at", isoformat_to_timestamp, 0),
        completed_at=_map_get(details, "completed_at", isoformat_to_timestamp, 0),
        outputs={
            file: file_details["privacy_level"] for file, file_details in files.items()
        },
    )


def _log(message):
    print(message, file=sys.stderr)


def _map_get(mapping, key, func, default):
    if key not in mapping:
        return default
    return func(mapping[key])
