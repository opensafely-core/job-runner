from pathlib import Path
from unittest.mock import patch

import pytest

from jobrunner.database import find_where, get_connection
from jobrunner.jobrequest import create_or_update_jobs


@pytest.fixture(autouse=True)
def temp_db(monkeypatch, tmp_path):
    monkeypatch.setattr("jobrunner.config.DATABASE_FILE", tmp_path / "db.sqlite")
    get_connection.cache_clear()


@patch("jobrunner.jobrequest.read_file_from_repo")
@patch("jobrunner.jobrequest.get_sha_from_remote_ref")
def test_create_or_update_jobs(get_sha_from_remote_ref, read_file_from_repo):
    get_sha_from_remote_ref.return_value = "abcdef123"
    read_file_from_repo.return_value = _read_file("fixtures/project.yaml")

    # fmt: off
    job_request = {
        "pk": "123",
        "workspace": {
            "repo": "https://github.com/opensafely/_no_such_repo",
            "branch": "develop",
        },
        "action_id": "run_model",
        "workspace_id": "1",
    }
    # fmt: on
    create_or_update_jobs(job_request)
    jobs = find_where("job")
    for job in jobs:
        del job["id"]
    assert jobs == [
        {
            "job_request_id": "123",
            "status": "P",
            "repo_url": "https://github.com/opensafely/_no_such_repo",
            "sha": "abcdef123",
            "workspace": "1",
            "action": "run_model",
            "wait_for_job_ids_json": [],
            "requires_outputs_from_json": ["generate_cohorts"],
            "run_command": "stata-mp:1.0 analysis/model.do",
            "output_spec_json": {"moderately_sensitive": {"log": "model.log"}},
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
