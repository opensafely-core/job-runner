from pathlib import Path
from unittest.mock import patch

import pytest

from jobrunner.database import find_where, get_connection
from jobrunner.jobrequest import create_or_update_jobs
from jobrunner.sync import job_request_from_remote_format


@pytest.fixture(autouse=True)
def temp_db_and_git_repo_dir(monkeypatch, tmp_path):
    monkeypatch.setattr("jobrunner.config.DATABASE_FILE", tmp_path / "db.sqlite")
    get_connection.cache_clear()
    monkeypatch.setattr("jobrunner.config.GIT_REPO_DIR", tmp_path / "repos")


def test_create_or_update_jobs():
    repo_url = str(Path(__file__).parent.resolve() / "fixtures/git-repo")
    # fmt: off
    job_request = {
        "pk": "123",
        "workspace": {
            "repo": repo_url,
            "branch": "v1",
        },
        "action_id": "generate_cohort",
        "workspace_id": "1",
    }
    # fmt: on
    job_request = job_request_from_remote_format(job_request)
    create_or_update_jobs(job_request)
    jobs = find_where("job")
    for job in jobs:
        del job["id"]
    assert jobs == [
        {
            "job_request_id": "123",
            "status": "P",
            "repo_url": repo_url,
            "commit": "d1e88b31cbe8f67c58f938adb5ee500d54a69764",
            "workspace": "1",
            "action": "generate_cohort",
            "wait_for_job_ids_json": [],
            "requires_outputs_from_json": [],
            "run_command": "cohortextractor:latest generate_cohort",
            "output_spec_json": {"highly_sensitive": {"cohort": "input.csv"}},
            "output_files_json": None,
            "error_message": None,
        }
    ]


@patch("jobrunner.jobrequest.read_file_from_repo")
def test_create_or_update_jobs_with_invalid_yaml(read_file_from_repo):
    read_file_from_repo.return_value = b"{}"

    # fmt: off
    job_request = {
        "pk": "234",
        "workspace": {
            "repo": "https://github.com/opensafely/_no_such_repo",
        },
        "action_id": "run_model",
        "workspace_id": "1",
        "commit": "abcdef123",
    }
    # fmt: on
    job_request = job_request_from_remote_format(job_request)
    create_or_update_jobs(job_request)
    jobs = find_where("job")
    assert len(jobs) == 1
    job = jobs[0]
    assert job["job_request_id"] == "234"
    assert job["status"] == "F"
    assert job["error_message"] == (
        "ProjectValidationError: Project file must specify a "
        "valid version (currently only 1.0)"
    )


def test_create_or_update_jobs_with_bad_git_repo():
    # fmt: off
    job_request = {
        "pk": "234",
        "workspace": {
            "repo": "https://github.com/opensafely/_no_such_repo",
        },
        "action_id": "run_model",
        "workspace_id": "1",
        "commit": "abcdef123",
    }
    # fmt: on
    job_request = job_request_from_remote_format(job_request)
    create_or_update_jobs(job_request)
    jobs = find_where("job")
    assert len(jobs) == 1
    job = jobs[0]
    assert job["job_request_id"] == "234"
    assert job["status"] == "F"
    assert job["error_message"] == (
        "GitError: Error fetching commit abcdef123 from "
        "https://github.com/opensafely/_no_such_repo"
    )


def _read_file(filename):
    with open(Path(__file__).parent / filename, "rb") as f:
        return f.read()
