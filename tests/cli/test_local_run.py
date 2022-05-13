import argparse
import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from jobrunner import config
from jobrunner.cli import local_run
from jobrunner.lib import database
from jobrunner.lib.subprocess_utils import subprocess_run
from jobrunner.manage_jobs import MANIFEST_FILE, METADATA_DIR
from jobrunner.models import Job, SavedJobRequest, State
from jobrunner.project import get_action_specification, parse_and_validate_project_file


FIXTURE_DIR = Path(__file__).parents[1].resolve() / "fixtures"


@pytest.fixture
def use_api(request, monkeypatch):
    monkeypatch.setattr(config, "EXECUTION_API", request.param)


@pytest.mark.parametrize("use_api", [True, False], indirect=True)
@pytest.mark.parametrize("extraction_tool", ["cohortextractor", "databuilder"])
@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_local_run_success(use_api, extraction_tool, tmp_path, docker_cleanup):
    project_dir = tmp_path / "project"
    shutil.copytree(str(FIXTURE_DIR / "full_project"), project_dir)

    local_run.main(project_dir=project_dir, actions=[f"analyse_data_{extraction_tool}"])

    # FIXME: consolidate these when databuilder supports more columns in dummy data
    if extraction_tool == "cohortextractor":
        paths = [
            "output/input.csv",
            "cohortextractor-counts.txt",
            "metadata/analyse_data_cohortextractor.log",
            "metadata/db.sqlite",
        ]
    else:
        paths = [
            "output/dataset.csv",
            "output/count_by_year.csv",
            "metadata/analyse_data_databuilder.log",
            "metadata/db.sqlite",
        ]

    for path in paths:
        assert (project_dir / path).exists(), path
    assert not (project_dir / "metadata/.logs").exists()


@pytest.mark.slow_test
@pytest.mark.needs_docker
@pytest.mark.skipif(
    not os.environ.get("STATA_LICENSE"), reason="No STATA_LICENSE env var"
)
def test_local_run_stata(tmp_path, monkeypatch, docker_cleanup):
    project_dir = tmp_path / "project"
    shutil.copytree(str(FIXTURE_DIR / "stata_project"), project_dir)
    monkeypatch.setattr("jobrunner.config.STATA_LICENSE", os.environ["STATA_LICENSE"])
    local_run.main(project_dir=project_dir, actions=["stata"])
    env_file = project_dir / "output/env.txt"
    assert "Bennett Institute" in env_file.read_text()


@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_local_run_triggers_a_manifest_migration(tmp_path, docker_cleanup):
    project_dir = tmp_path / "project"
    shutil.copytree(str(FIXTURE_DIR / "full_project"), project_dir)

    # This action doesn't exist in the project.yaml, but the migration doesn't care. We use this instead of an action
    # that does exist so that it's unambiguous that the database record had been created by the migration rather than
    # as a side-effect of running the action we specify.
    manifest = {
        "workspace": "the-workspace",
        "repo": "the-repo-url",
        "actions": {"the-action": {"job_id": "job-id-from-manifest"}},
        "files": {},
    }
    manifest_file = project_dir / METADATA_DIR / MANIFEST_FILE
    manifest_file.parent.mkdir(parents=True)
    manifest_file.write_text(json.dumps(manifest))

    local_run.main(project_dir=project_dir, actions=["generate_cohort"])

    assert database.exists_where(Job, id="job-id-from-manifest")


@pytest.mark.slow_test
@pytest.mark.needs_docker
@pytest.mark.parametrize("extraction_tool", ["cohortextractor", "databuilder"])
def test_local_run_copes_with_detritus_of_earlier_interrupted_run(
    extraction_tool, tmp_path
):
    # This test simulates the case where an earlier run has been interrupted (for example by the user pressing ctrl-c).
    # In particular we put a couple of jobs in unfinished states, which they could never be left in under normal
    # operation. The correct behaviour of the local run, which this tests for, is for such unfinished jobs to be marked
    # as cancelled on the next run.
    project_dir = tmp_path / "project"
    shutil.copytree(str(FIXTURE_DIR / "full_project"), project_dir)
    config.DATABASE_FILE = project_dir / "metadata" / "db.sqlite"

    project = parse_and_validate_project_file(
        (project_dir / "project.yaml").read_bytes()
    )
    database.insert(SavedJobRequest(id="previous-request", original={}))

    def job(job_id, action, state):
        spec = get_action_specification(project, action)
        return Job(
            id=job_id,
            job_request_id="previous-request",
            state=state,
            status_message="",
            repo_url=str(project_dir),
            workspace=project_dir.name,
            database_name="a-database",
            action=action,
            wait_for_job_ids=[],
            requires_outputs_from=spec.needs,
            run_command=spec.run,
            output_spec=spec.outputs,
            created_at=int(time.time()),
            updated_at=int(time.time()),
            outputs={},
        )

    # FIXME: consolidate these when databuilder supports more columns in dummy data
    if extraction_tool == "cohortextractor":
        actions = ["generate_cohort", "prepare_data_m_cohortextractor"]
    else:
        actions = ["generate_dataset", "analyse_data_databuilder"]

    database.insert(job(job_id="123", action=actions[0], state=State.RUNNING))
    database.insert(job(job_id="456", action=actions[1], state=State.PENDING))
    assert local_run.main(project_dir=project_dir, actions=[actions[1]])

    assert database.find_one(Job, id="123").cancelled
    assert database.find_one(Job, id="123").state == State.FAILED
    assert database.find_one(Job, id="456").cancelled
    assert database.find_one(Job, id="456").state == State.FAILED


@pytest.fixture
def systmpdir(monkeypatch, tmp_path):
    """Set the system tempdir to tmp_path for this test, for isolation."""
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))


@pytest.fixture
def license_repo(tmp_path):
    # create a repo to clone the license from
    repo = tmp_path / "test-repo"
    repo.mkdir()
    license = repo / "stata.lic"  # noqa: A001
    license.write_text("repo-license")
    git = ["git", "-c", "user.name=test", "-c", "user.email=test@example.com"]
    env = {"GIT_CONFIG_GLOBAL": "/dev/null"}
    repo_path = str(repo)
    subprocess_run(git + ["init"], cwd=repo_path, env=env)
    subprocess_run(git + ["add", "stata.lic"], cwd=repo_path, env=env)
    subprocess_run(
        git + ["commit", "--no-gpg-sign", "-m", "test"], cwd=repo_path, env=env
    )
    return repo_path


def test_get_stata_license_cache_recent(systmpdir, monkeypatch, tmp_path):
    def fail(*a, **kwargs):
        assert False, "should not have been called"

    monkeypatch.setattr("jobrunner.lib.subprocess_utils.subprocess_run", fail)
    cache = tmp_path / "opensafely-stata.lic"
    cache.write_text("cached-license")
    assert local_run.get_stata_license() == "cached-license"


def test_get_stata_license_cache_expired(systmpdir, tmp_path, license_repo):
    cache = tmp_path / "opensafely-stata.lic"
    cache.write_text("cached-license")
    utime = (datetime.utcnow() - timedelta(hours=12)).timestamp()
    os.utime(cache, (utime, utime))

    assert local_run.get_stata_license(license_repo) == "repo-license"
    assert (tmp_path / "opensafely-stata.lic").read_text() == "repo-license"


def test_get_stata_license_repo_fetch(systmpdir, tmp_path, license_repo):
    assert local_run.get_stata_license(license_repo) == "repo-license"
    assert (tmp_path / "opensafely-stata.lic").read_text() == "repo-license"


def test_get_stata_license_repo_error(systmpdir):
    assert local_run.get_stata_license("/invalid/repo") is None


def test_add_arguments():
    parser = argparse.ArgumentParser(description="test")
    parser = local_run.add_arguments(parser)
    args = parser.parse_args(
        [
            "action",
            "--force-run-dependencies",
            "--project-dir=dir",
            "--continue-on-error",
            "--timestamps",
            "--debug",
        ]
    )

    assert args.actions == ["action"]
    assert args.force_run_dependencies
    assert args.project_dir == "dir"
    assert args.timestamps
    assert args.debug


def test_filter_log_messages():
    record = logging.makeLogRecord({})
    assert local_run.filter_log_messages(record)

    record = logging.makeLogRecord({"status_code": "code"})
    assert local_run.filter_log_messages(record)

    record = logging.makeLogRecord(
        {"status_code": local_run.StatusCode.WAITING_ON_DEPENDENCIES}
    )
    assert local_run.filter_log_messages(record) is False
