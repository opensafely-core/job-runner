import json

import pytest

from jobrunner import config
from jobrunner.cli import manifest_migration
from jobrunner.lib import database
from jobrunner.manage_jobs import MANIFEST_FILE, METADATA_DIR
from jobrunner.models import Job


def test_triggers_migration(tmp_work_dir):
    write_manifest(
        {
            "workspace": "the-workspace",
            "actions": {"the-action": {"job_id": "job-id-from-manifest"}},
            "files": {},
        }
    )

    manifest_migration.main()

    assert database.exists_where(Job, id="job-id-from-manifest")


def test_passes_on_batch_size(tmp_work_dir):
    write_manifest(
        {
            "workspace": "the-workspace",
            "actions": {f"action-{i}": {"job_id": f"job-{i}"} for i in range(100)},
            "files": {},
        }
    )

    manifest_migration.main(["--batch-size", "5"])

    assert len(database.find_where(Job, workspace="the-workspace")) == 5


def test_defaults_batch_size_to_one(tmp_work_dir):
    write_manifest(
        {
            "workspace": "the-workspace",
            "actions": {f"action-{i}": {"job_id": f"job-{i}"} for i in range(5)},
            "files": {},
        }
    )

    manifest_migration.main()

    assert len(database.find_where(Job, workspace="the-workspace")) == 1


def test_passes_on_dry_run(tmp_work_dir):
    write_manifest(
        {
            "workspace": "the-workspace",
            "actions": {"the-action": {"job_id": "job-id-from-manifest"}},
            "files": {},
        }
    )

    manifest_migration.main(["--dry-run"])

    assert not database.exists_where(Job, id="job-id-from-manifest")


def test_ignores_errors_for_dry_run(tmp_work_dir):
    # This manifest is invalid because it doesn't contain an "actions" element.
    write_manifest(
        {
            "workspace": "the-workspace",
            "files": {},
        }
    )

    # Future-proof the test against implementation changes. If this fails it's because the manifest above is no longer
    # invalid, so we need to reformulate the test.
    with pytest.raises(KeyError):
        manifest_migration.main()

    # No exception should be raised with --dry-run.
    manifest_migration.main(["--dry-run"])


def write_manifest(manifest):
    manifest_file = (
        config.HIGH_PRIVACY_WORKSPACES_DIR
        / "the-workspace"
        / METADATA_DIR
        / MANIFEST_FILE
    )
    manifest_file.parent.mkdir(parents=True)
    manifest_file.write_text(json.dumps(manifest))
