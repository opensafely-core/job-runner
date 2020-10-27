from pathlib import Path

import pytest

from jobrunner.database import find_where, get_connection
from jobrunner.models import JobRequest, Job, State
from jobrunner.create_or_update_jobs import create_or_update_jobs


@pytest.fixture(autouse=True)
def temp_db_and_git_repo_dir(monkeypatch, tmp_path):
    monkeypatch.setattr("jobrunner.config.DATABASE_FILE", tmp_path / "db.sqlite")
    get_connection.cache_clear()
    monkeypatch.setattr("jobrunner.config.GIT_REPO_DIR", tmp_path / "repos")


def test_create_or_update_jobs():
    repo_url = str(Path(__file__).parent.resolve() / "fixtures/git-repo")
    job_request = JobRequest(
        id="123",
        repo_url=repo_url,
        commit=None,
        branch="v1",
        action="generate_cohort",
        workspace="1",
        original={},
    )
    create_or_update_jobs(job_request)
    jobs = find_where(Job)
    assert jobs == [
        Job(
            # This is a UUID so we can't predict its value
            id=jobs[0].id,
            job_request_id="123",
            status=State.PENDING,
            repo_url=repo_url,
            commit="d1e88b31cbe8f67c58f938adb5ee500d54a69764",
            workspace="1",
            action="generate_cohort",
            wait_for_job_ids=[],
            requires_outputs_from=[],
            run_command="cohortextractor:latest generate_cohort",
            output_spec={"highly_sensitive": {"cohort": "input.csv"}},
            output_files=None,
            error_message=None,
        )
    ]
    # Check no new jobs created from same JobRequest
    create_or_update_jobs(job_request)
    new_jobs = find_where(Job)
    assert jobs == new_jobs


def test_create_or_update_jobs_with_git_error():
    repo_url = str(Path(__file__).parent.resolve() / "fixtures/git-repo")
    job_request = JobRequest(
        id="123",
        repo_url=repo_url,
        commit=None,
        branch="no-such-branch",
        action="generate_cohort",
        workspace="1",
        original={},
    )
    create_or_update_jobs(job_request)
    jobs = find_where(Job)
    assert jobs == [
        Job(
            # This is a UUID so we can't predict its value
            id=jobs[0].id,
            job_request_id="123",
            status=State.FAILED,
            repo_url=repo_url,
            commit=None,
            workspace="1",
            action="generate_cohort",
            wait_for_job_ids=None,
            requires_outputs_from=None,
            run_command=None,
            output_spec=None,
            output_files=None,
            error_message=f"GitError: Error resolving ref 'no-such-branch' from {repo_url}",
        )
    ]
