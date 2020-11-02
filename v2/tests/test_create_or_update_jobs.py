from pathlib import Path
import uuid

import pytest

from jobrunner.database import find_where, get_connection
from jobrunner.models import JobRequest, Job, State
from jobrunner.create_or_update_jobs import (
    create_or_update_jobs,
    create_jobs_with_project_file,
)


@pytest.fixture(autouse=True)
def setup(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "jobrunner.create_or_update_jobs.outputs_exist", lambda *args: False
    )
    monkeypatch.setattr("jobrunner.config.DATABASE_FILE", tmp_path / "db.sqlite")
    monkeypatch.setattr("jobrunner.config.GIT_REPO_DIR", tmp_path / "repos")
    get_connection.cache_clear()


# Basic smoketest to test the full execution path
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
            run_command="docker.opensafely.org/cohortextractor:latest generate_cohort",
            output_spec={"highly_sensitive": {"cohort": "input.csv"}},
            output_files=None,
            error_message=None,
        )
    ]
    # Check no new jobs created from same JobRequest
    create_or_update_jobs(job_request)
    new_jobs = find_where(Job)
    assert jobs == new_jobs


# Basic smoketest to test the error path
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


TEST_PROJECT = """
version: '1.0'
actions:
  generate_cohort:
    run: cohortextractor:latest generate_cohort
    outputs:
      highly_sensitive:
        cohort: input.csv

  prepare_data_1:
    run: stata-mp:latest analysis/prepare_data_1.do
    needs: [generate_cohort]
    outputs:
      highly_sensitive:
        data: prepared_1.dta

  prepare_data_2:
    run: stata-mp:latest analysis/prepare_data_2.do
    needs: [generate_cohort]
    outputs:
      highly_sensitive:
        data: prepared_2.dta

  analyse_data:
    run: stata-mp:latest analysis/analyse_data.do
    needs: [prepare_data_1, prepare_data_2]
    outputs:
      moderately_sensitive:
        analysis: analysis.txt
"""


def test_adding_job_creates_dependencies():
    create_jobs_with_project_file(make_job_request(action="analyse_data"), TEST_PROJECT)
    analyse_job = find_where(Job, action="analyse_data")[0]
    prepare_1_job = find_where(Job, action="prepare_data_1")[0]
    prepare_2_job = find_where(Job, action="prepare_data_2")[0]
    generate_job = find_where(Job, action="generate_cohort")[0]
    assert set(analyse_job.wait_for_job_ids) == {prepare_1_job.id, prepare_2_job.id}
    assert prepare_1_job.wait_for_job_ids == [generate_job.id]
    assert prepare_2_job.wait_for_job_ids == [generate_job.id]
    assert generate_job.wait_for_job_ids == []


def test_existing_active_jobs_are_picked_up_when_checking_dependencies():
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_where(Job, action="prepare_data_1")[0]
    generate_job = find_where(Job, action="generate_cohort")[0]
    assert prepare_1_job.wait_for_job_ids == [generate_job.id]
    # Now schedule a job which has the above jobs as dependencies
    create_jobs_with_project_file(make_job_request(action="analyse_data"), TEST_PROJECT)
    # Check that it's waiting on the existing jobs
    analyse_job = find_where(Job, action="analyse_data")[0]
    prepare_2_job = find_where(Job, action="prepare_data_2")[0]
    assert set(analyse_job.wait_for_job_ids) == {prepare_1_job.id, prepare_2_job.id}
    assert prepare_2_job.wait_for_job_ids == [generate_job.id]


def make_job_request(**kwargs):
    job_request = JobRequest(
        id=str(uuid.uuid4()),
        repo_url="https://example.com/repo.git",
        commit="abcdef0123456789",
        workspace="1",
        action="generate_cohort",
        original={},
    )
    for key, value in kwargs.items():
        setattr(job_request, key, value)
    return job_request
