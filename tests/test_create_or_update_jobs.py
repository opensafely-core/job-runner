import uuid
from pathlib import Path
from unittest import mock

import pytest

from jobrunner.create_or_update_jobs import (
    JobRequestError,
    NothingToDoError,
    create_jobs,
    create_or_update_jobs,
    validate_job_request,
)
from jobrunner.lib.database import find_one, update_where
from jobrunner.models import Job, JobRequest, State


@pytest.fixture(autouse=True)
def disable_github_org_checking(monkeypatch):
    monkeypatch.setattr("jobrunner.config.ALLOWED_GITHUB_ORGS", None)


# Basic smoketest to test the full execution path
def test_create_or_update_jobs(tmp_work_dir):
    repo_url = str(Path(__file__).parent.resolve() / "fixtures/git-repo")
    job_request = JobRequest(
        id="123",
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit="d1e88b31cbe8f67c58f938adb5ee500d54a69764",
        branch="v1",
        requested_actions=["generate_cohort"],
        cancelled_actions=[],
        workspace="1",
        database_name="dummy",
        original={},
    )
    create_or_update_jobs(job_request)
    old_job = find_one(Job)
    assert old_job.job_request_id == "123"
    assert old_job.state == State.PENDING
    assert old_job.repo_url == repo_url
    assert old_job.commit == "d1e88b31cbe8f67c58f938adb5ee500d54a69764"
    assert old_job.workspace == "1"
    assert old_job.action == "generate_cohort"
    assert old_job.wait_for_job_ids == []
    assert old_job.requires_outputs_from == []
    assert old_job.run_command == (
        "cohortextractor:latest generate_cohort --expectations-population=1000"
        " --output-dir=."
    )
    assert old_job.output_spec == {"highly_sensitive": {"cohort": "input.csv"}}
    assert old_job.status_message is None
    # Check no new jobs created from same JobRequest
    create_or_update_jobs(job_request)
    new_job = find_one(Job)
    assert old_job == new_job


# Basic smoketest to test the error path
def test_create_or_update_jobs_with_git_error(tmp_work_dir):
    repo_url = str(Path(__file__).parent.resolve() / "fixtures/git-repo")
    bad_commit = "0" * 40
    job_request = JobRequest(
        id="123",
        repo_url=repo_url,
        commit=bad_commit,
        branch="v1",
        requested_actions=["generate_cohort"],
        cancelled_actions=[],
        workspace="1",
        database_name="dummy",
        original={},
    )
    create_or_update_jobs(job_request)
    j = find_one(Job)
    assert j.job_request_id == "123"
    assert j.state == State.FAILED
    assert j.repo_url == repo_url
    assert j.commit == bad_commit
    assert j.workspace == "1"
    assert j.wait_for_job_ids is None
    assert j.requires_outputs_from is None
    assert j.run_command is None
    assert j.output_spec is None
    assert (
        j.status_message
        == f"GitError: Error fetching commit {bad_commit} from {repo_url}"
    )


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


def test_adding_job_creates_dependencies(tmp_work_dir):
    create_jobs_with_project_file(make_job_request(action="analyse_data"), TEST_PROJECT)
    analyse_job = find_one(Job, action="analyse_data")
    prepare_1_job = find_one(Job, action="prepare_data_1")
    prepare_2_job = find_one(Job, action="prepare_data_2")
    generate_job = find_one(Job, action="generate_cohort")
    assert set(analyse_job.wait_for_job_ids) == {prepare_1_job.id, prepare_2_job.id}
    assert prepare_1_job.wait_for_job_ids == [generate_job.id]
    assert prepare_2_job.wait_for_job_ids == [generate_job.id]
    assert generate_job.wait_for_job_ids == []


def test_existing_active_jobs_are_picked_up_when_checking_dependencies(tmp_work_dir):
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_cohort")
    assert prepare_1_job.wait_for_job_ids == [generate_job.id]
    # Now schedule a job which has the above jobs as dependencies
    create_jobs_with_project_file(make_job_request(action="analyse_data"), TEST_PROJECT)
    # Check that it's waiting on the existing jobs
    analyse_job = find_one(Job, action="analyse_data")
    prepare_2_job = find_one(Job, action="prepare_data_2")
    assert set(analyse_job.wait_for_job_ids) == {prepare_1_job.id, prepare_2_job.id}
    assert prepare_2_job.wait_for_job_ids == [generate_job.id]


def test_existing_cancelled_jobs_are_ignored_up_when_checking_dependencies(
    tmp_work_dir,
):
    create_jobs_with_project_file(
        make_job_request(action="generate_cohort"), TEST_PROJECT
    )
    cancelled_generate_job = find_one(Job, action="generate_cohort")
    update_where(Job, {"cancelled": True}, id=cancelled_generate_job.id)

    # Now schedule a job which has the above job as a dependency
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )

    # Check that it's spawned a new instance of the cancelled job and wired up the dependencies correctly
    prepare_job = find_one(Job, action="prepare_data_1")
    new_generate_job = find_one(Job, action="generate_cohort", cancelled=0)
    assert new_generate_job.id != cancelled_generate_job.id

    assert len(prepare_job.wait_for_job_ids) == 1
    assert prepare_job.wait_for_job_ids[0] == new_generate_job.id


def test_run_all_ignores_failed_actions_that_have_been_removed(tmp_work_dir):
    # Long ago there was an useless action that failed and then was rightly expunged from the study pipeline
    obsolete_action_def = """
  obsolete_action:
    run: python:latest -c pass
    outputs: {}
    """
    create_jobs_with_project_file(
        make_job_request(action="obsolete_action"), TEST_PROJECT + obsolete_action_def
    )
    update_where(Job, {"state": State.FAILED}, action="obsolete_action")

    # Since then all the healthy, vigorous actions have been successfully run individually
    request = make_job_request(
        actions=["generate_cohort", "prepare_data_1", "prepare_data_2", "analyse_data"]
    )
    create_jobs_with_project_file(request, TEST_PROJECT)
    update_where(Job, {"state": State.SUCCEEDED}, job_request_id=request.id)

    with pytest.raises(NothingToDoError):
        # Now this should be a no-op because all the actions that are still part of the study have succeeded
        create_jobs_with_project_file(make_job_request(action="run_all"), TEST_PROJECT)


def test_cancelled_jobs_are_flagged(tmp_work_dir):
    job_request = make_job_request(action="analyse_data")
    create_jobs_with_project_file(job_request, TEST_PROJECT)
    job_request.cancelled_actions = ["prepare_data_1", "prepare_data_2"]
    create_or_update_jobs(job_request)
    analyse_job = find_one(Job, action="analyse_data")
    prepare_1_job = find_one(Job, action="prepare_data_1")
    prepare_2_job = find_one(Job, action="prepare_data_2")
    generate_job = find_one(Job, action="generate_cohort")
    assert analyse_job.cancelled == 0
    assert prepare_1_job.cancelled == 1
    assert prepare_2_job.cancelled == 1
    assert generate_job.cancelled == 0


@pytest.mark.parametrize(
    "params,exc_msg",
    [
        ({"workspace": None}, "Workspace name cannot be blank"),
        ({"workspace": "$%#"}, "Invalid workspace"),
        ({"database_name": "invalid"}, "Invalid database name"),
    ],
)
def test_validate_job_request(params, exc_msg, monkeypatch):
    monkeypatch.setattr("jobrunner.config.USING_DUMMY_DATA_BACKEND", False)
    repo_url = str(Path(__file__).parent.resolve() / "fixtures/git-repo")
    kwargs = dict(
        id="123",
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit="d1e88b31cbe8f67c58f938adb5ee500d54a69764",
        branch="v1",
        requested_actions=["generate_cohort"],
        cancelled_actions=[],
        workspace="1",
        database_name="full",  # note db from from job-server is 'full'
        original={},
    )
    kwargs.update(params)
    job_request = JobRequest(**kwargs)

    with pytest.raises(JobRequestError, match=exc_msg):
        validate_job_request(job_request)


def make_job_request(action=None, actions=None, **kwargs):
    assert not (actions and action)
    if not actions:
        if action:
            actions = [action]
        else:
            actions = ["generate_cohort"]
    job_request = JobRequest(
        id=str(uuid.uuid4()),
        repo_url="https://example.com/repo.git",
        commit="abcdef0123456789",
        workspace="1",
        database_name="full",
        requested_actions=actions,
        cancelled_actions=[],
        original={},
    )
    for key, value in kwargs.items():
        setattr(job_request, key, value)
    return job_request


def create_jobs_with_project_file(job_request, project_file):
    with mock.patch("jobrunner.create_or_update_jobs.get_project_file") as f:
        f.return_value = project_file
        return create_jobs(job_request)
