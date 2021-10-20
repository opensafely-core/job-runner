import json
import sys
from contextlib import contextmanager

from jobrunner import config, models
from jobrunner.lib import database
from jobrunner.manage_jobs import MANIFEST_FILE, METADATA_DIR
from jobrunner.models import Job, State, isoformat_to_timestamp


def migrate_all(batch_size=10, log=True, dry_run=False, ignore_errors=False):
    _migrate(
        sorted(
            config.HIGH_PRIVACY_WORKSPACES_DIR.iterdir()
        ),  # sorted to make testing easier
        batch_size,
        write_medium_privacy_manifest=True,
        log=log,
        dry_run=dry_run,
        ignore_errors=ignore_errors,
    )


def migrate_one(
    workspace_dir,
    write_medium_privacy_manifest,
    batch_size=10,
    log=True,
    dry_run=False,
    ignore_errors=False,
):
    _migrate(
        [workspace_dir],
        batch_size,
        write_medium_privacy_manifest,
        log,
        dry_run,
        ignore_errors,
    )


def _migrate(
    workspace_dirs,
    batch_size,
    write_medium_privacy_manifest,
    log,
    dry_run,
    ignore_errors,
):
    count = 0

    for job in _jobs_from_workspaces(
        workspace_dirs, write_medium_privacy_manifest, log, dry_run, ignore_errors
    ):
        if count >= batch_size:
            _log(
                f"Reached batch size of {batch_size}. There are more jobs to be migrated.",
                log,
            )
            break

        if database.exists_where(Job, id=job.id):
            continue

        _insert_in_database(job, log, dry_run)
        count += 1

    if count == 0:
        _log("There were no jobs to migrate.", log)


def _jobs_from_workspaces(
    workspace_dirs, write_medium_privacy_manifest, log, dry_run, ignore_errors
):
    for workspace_dir in workspace_dirs:
        with _possibly_ignored_errors(log, ignore_errors, workspace=workspace_dir.name):
            yield from _jobs_from_workspace(
                workspace_dir,
                write_medium_privacy_manifest,
                log,
                dry_run,
                ignore_errors,
            )


def _jobs_from_workspace(
    workspace_dir, write_medium_privacy_manifest, log, dry_run, ignore_errors
):
    manifest_file = workspace_dir / METADATA_DIR / MANIFEST_FILE
    if not manifest_file.exists():
        return

    manifest = json.load(manifest_file.open())
    workspace_name = manifest.get("workspace", workspace_dir.name)
    repo = manifest.get("repo")
    all_files = manifest.get("files", {}).items()
    _log(f"Migrating workspace {workspace_name} in directory {workspace_dir}.", log)

    for action, action_details in manifest["actions"].items():
        with _possibly_ignored_errors(
            log, ignore_errors, workspace=workspace_name, action=action
        ):
            files = {
                file: file_details
                for file, file_details in all_files
                if file_details["created_by_action"] == action
            }
            yield _action_to_job(workspace_name, repo, files, action, action_details)

    _migrate_manifest_files(
        manifest_file, repo, workspace_name, write_medium_privacy_manifest, dry_run
    )


def _migrate_manifest_files(
    manifest, repo, workspace, write_medium_privacy_manifest, dry_run
):
    if dry_run:
        return

    if write_medium_privacy_manifest:
        _migrate_medium_privacy_manifest(repo, workspace)

    # We're done with this manifest. Move it out of the way so it doesn't cause confusion -- it's no longer being
    # updated, so soon it will be out of date.
    manifest.rename(manifest.with_name(f".deprecated.{MANIFEST_FILE}"))


def _migrate_medium_privacy_manifest(repo, workspace):
    # osrelease needs to be able to read the workspace name and repo URL from somewhere, in order to avoid the
    # person doing the release having to enter all the details. So we write this rump manifest just into the
    # medium privacy workspace. release-hatch is launched with this information already provided, so when osrelease
    # has been removed we can remove these files.
    manifest = (
        config.MEDIUM_PRIVACY_WORKSPACES_DIR / workspace / METADATA_DIR / MANIFEST_FILE
    )
    manifest.parent.mkdir(exist_ok=True, parents=True)
    manifest.write_text(json.dumps({"workspace": workspace, "repo": repo}))


def _action_to_job(workspace, repo, files, action, details):
    job_id = details["job_id"]
    if job_id == "unknown":
        # There are lots of historical jobs with their id recorded in the manifest as literally "unknown". The id is a
        # primary key, so we need unique values. But we don't need to join it to anything, so a random value is fine.
        job_id = models.random_id()

    return Job(
        id=job_id,
        workspace=workspace,
        repo_url=repo,
        action=action,
        state=_map_get(details, "state", State, None),
        commit=details.get("commit"),
        image_id=details.get("docker_image_id"),
        created_at=_map_get(details, "created_at", isoformat_to_timestamp, 0),
        completed_at=_map_get(details, "completed_at", isoformat_to_timestamp, 0),
        outputs={
            file: file_details["privacy_level"] for file, file_details in files.items()
        },
    )


def _insert_in_database(job, log, dry_run):
    if dry_run:
        log_prefix = "Dry run. Would have inserted"
    else:
        database.insert(job)
        log_prefix = "Inserted"
    _log(f"{log_prefix} Job(id={job.id}, action={job.action}).", log)


@contextmanager
def _possibly_ignored_errors(log, ignore_errors, **context):
    try:
        yield
    except Exception as e:
        if ignore_errors:
            _log(
                f"Ignoring {e.__class__.__name__}({e}) in {context}.",
                log,
            )
        else:
            raise


def _log(message, log):
    if log:
        print(message, file=sys.stderr)


def _map_get(mapping, key, func, default):
    try:
        return func(mapping[key])
    except (ValueError, KeyError):
        return default
