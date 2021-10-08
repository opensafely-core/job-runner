import json

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

    manifest_migration.main(["--batch-size", "10"])

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


def write_manifest(manifest):
    manifest_file = (
        config.HIGH_PRIVACY_WORKSPACES_DIR
        / "the-workspace"
        / METADATA_DIR
        / MANIFEST_FILE
    )
    manifest_file.parent.mkdir(parents=True)
    manifest_file.write_text(json.dumps(manifest))
