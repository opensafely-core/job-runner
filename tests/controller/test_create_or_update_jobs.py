import re
import uuid
from pathlib import Path
from unittest import mock

import pytest

from common.lib.github_validators import GithubValidationError
from controller.create_or_update_jobs import (
    JobRequestError,
    NothingToDoError,
    StaleCodelistError,
    create_job_from_exception,
    create_jobs,
    create_or_update_jobs,
    validate_job_request,
)
from controller.lib.database import count_where, find_one, find_where, update_where
from controller.models import Job, JobRequest, State, StatusCode
from tests.conftest import get_trace
from tests.factories import job_request_factory_raw


FIXTURES_PATH = Path(__file__).parent.parent.resolve() / "fixtures"


@pytest.fixture(autouse=True)
def disable_github_org_checking(monkeypatch):
    monkeypatch.setattr("controller.config.ALLOWED_GITHUB_ORGS", None)


# Basic smoketest to test the full execution path
def test_create_or_update_jobs(tmp_work_dir, db):
    repo_url = str(FIXTURES_PATH / "git-repo")
    job_request = JobRequest(
        id="123",
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit="d090466f63b0d68084144d8f105f0d6e79a0819e",
        branch="v1",
        requested_actions=["generate_dataset"],
        cancelled_actions=[],
        workspace="1",
        codelists_ok=True,
        database_name="default",
        original=dict(
            created_by="user",
            project="project",
            orgs=["org1", "org2"],
        ),
        backend="test",
    )
    create_or_update_jobs(job_request)
    old_job = find_one(Job)
    assert old_job.job_request_id == "123"
    assert old_job.state == State.PENDING
    assert old_job.repo_url == repo_url
    assert old_job.commit == "d090466f63b0d68084144d8f105f0d6e79a0819e"
    assert old_job.workspace == "1"
    assert old_job.action == "generate_dataset"
    assert old_job.wait_for_job_ids == []
    assert old_job.requires_outputs_from == []
    assert old_job.run_command == (
        "ehrql:v1 generate-dataset analysis/dataset_definition.py --output output/dataset.csv.gz"
    )
    assert old_job.output_spec == {
        "highly_sensitive": {"dataset": "output/dataset.csv.gz"}
    }
    assert old_job.backend == "test"
    assert old_job.status_message == "Created"
    # Check no new jobs created from same JobRequest
    create_or_update_jobs(job_request)
    new_job = find_one(Job)
    assert old_job == new_job


# Basic smoketest to test the error path
def test_create_or_update_jobs_with_git_error(tmp_work_dir):
    repo_url = str(FIXTURES_PATH / "git-repo")
    bad_commit = "0" * 40
    job_request = JobRequest(
        id="123",
        repo_url=repo_url,
        commit=bad_commit,
        branch="v1",
        requested_actions=["generate_dataset"],
        cancelled_actions=[],
        workspace="1",
        codelists_ok=True,
        database_name="default",
        original=dict(
            created_by="user",
            project="project",
            orgs=["org1", "org2"],
        ),
        backend="test",
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
    assert j.backend == "test"
    assert (
        j.status_message
        == f"GitError: Error fetching commit {bad_commit} from {repo_url}"
    )


@mock.patch(
    "controller.create_or_update_jobs.create_jobs", side_effect=Exception("unk")
)
def test_create_or_update_jobs_with_unhandled_error(tmp_work_dir, db):
    repo_url = str(FIXTURES_PATH / "git-repo")
    job_request = JobRequest(
        id="123",
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit="cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74",
        branch="v1",
        requested_actions=["generate_dataset"],
        cancelled_actions=[],
        workspace="1",
        codelists_ok=True,
        database_name="default",
        original=dict(
            created_by="user",
            project="project",
            orgs=["org1", "org2"],
        ),
        backend="test",
    )
    create_or_update_jobs(job_request)
    j = find_one(Job, job_request_id="123")
    assert j.job_request_id == "123"
    assert j.state == State.FAILED
    assert j.repo_url == repo_url
    assert j.commit == "cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74"
    assert j.workspace == "1"
    assert j.wait_for_job_ids is None
    assert j.requires_outputs_from is None
    assert j.run_command is None
    assert j.output_spec is None
    assert j.backend == "test"
    assert j.status_message == "JobRequestError: Internal error"


TEST_PROJECT = """
version: '1.0'
actions:
  generate_dataset:
    run: ehrql:v1 generate-dataset analysis/dataset_definition.py --output output.csv.gz
    outputs:
      highly_sensitive:
        cohort: output.csv.gz

  prepare_data_1:
    run: stata-mp:latest analysis/prepare_data_1.do
    needs: [generate_dataset]
    outputs:
      highly_sensitive:
        data: prepared_1.dta

  prepare_data_2:
    run: stata-mp:latest analysis/prepare_data_2.do
    needs: [generate_dataset]
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
    generate_job = find_one(Job, action="generate_dataset")
    assert set(analyse_job.wait_for_job_ids) == {prepare_1_job.id, prepare_2_job.id}
    assert prepare_1_job.wait_for_job_ids == [generate_job.id]
    assert prepare_2_job.wait_for_job_ids == [generate_job.id]
    assert generate_job.wait_for_job_ids == []


def test_existing_active_jobs_are_picked_up_when_checking_dependencies(tmp_work_dir):
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    assert prepare_1_job.wait_for_job_ids == [generate_job.id]
    # Now schedule a job which has the above jobs as dependencies
    create_jobs_with_project_file(make_job_request(action="analyse_data"), TEST_PROJECT)
    # Check that it's waiting on the existing jobs
    analyse_job = find_one(Job, action="analyse_data")
    prepare_2_job = find_one(Job, action="prepare_data_2")
    assert set(analyse_job.wait_for_job_ids) == {prepare_1_job.id, prepare_2_job.id}
    assert prepare_2_job.wait_for_job_ids == [generate_job.id]


def test_existing_active_jobs_for_other_backends_are_ignored_when_checking_dependencies(
    tmp_work_dir, monkeypatch
):
    monkeypatch.setattr("common.config.BACKENDS", ["foo", "bar"])
    # Schedule the same job on 2 backends
    create_jobs_with_project_file(
        make_job_request(action="analyse_data", backend="foo"), TEST_PROJECT
    )
    create_jobs_with_project_file(
        make_job_request(action="analyse_data", backend="bar"), TEST_PROJECT
    )

    # There are now 2 of each job
    for action in [
        "generate_dataset",
        "prepare_data_1",
        "prepare_data_2",
        "analyse_data",
    ]:
        assert count_where(Job, action=action) == 2

    # Check that they're waiting on the right existing jobs
    for backend in ["foo", "bar"]:
        generate_dataset_job = find_one(Job, action="generate_dataset", backend=backend)
        prepare_1_job = find_one(Job, action="prepare_data_1", backend=backend)
        prepare_2_job = find_one(Job, action="prepare_data_2", backend=backend)
        analyse_job = find_one(Job, action="analyse_data", backend=backend)

        assert set(prepare_1_job.wait_for_job_ids) == {generate_dataset_job.id}
        assert set(analyse_job.wait_for_job_ids) == {prepare_1_job.id, prepare_2_job.id}


def test_existing_succeeded_jobs_are_picked_up_when_checking_dependencies(tmp_work_dir):
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    assert prepare_1_job.wait_for_job_ids == [generate_job.id]
    # make the generate_job succeeded
    update_where(Job, {"state": State.SUCCEEDED}, id=generate_job.id)

    # Now schedule a job which has the above jobs as dependencies
    create_jobs_with_project_file(make_job_request(action="analyse_data"), TEST_PROJECT)
    # Check that it's waiting on the existing jobs
    analyse_job = find_one(Job, action="analyse_data")
    prepare_2_job = find_one(Job, action="prepare_data_2")
    assert set(analyse_job.wait_for_job_ids) == {prepare_1_job.id, prepare_2_job.id}
    # prepare_2 is only dependent on generate_job, which has succeeded
    assert prepare_2_job.wait_for_job_ids == []


def test_existing_cancelled_jobs_are_ignored_up_when_checking_dependencies(
    tmp_work_dir,
):
    create_jobs_with_project_file(
        make_job_request(action="generate_dataset"), TEST_PROJECT
    )
    cancelled_generate_job = find_one(Job, action="generate_dataset")
    update_where(Job, {"cancelled": True}, id=cancelled_generate_job.id)

    # Now schedule a job which has the above job as a dependency
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )

    # Check that it's spawned a new instance of the cancelled job and wired up the dependencies correctly
    prepare_job = find_one(Job, action="prepare_data_1")
    new_generate_job = find_one(Job, action="generate_dataset", cancelled=0)
    assert new_generate_job.id != cancelled_generate_job.id

    assert len(prepare_job.wait_for_job_ids) == 1
    assert prepare_job.wait_for_job_ids[0] == new_generate_job.id


def test_run_all_ignores_failed_actions_that_have_been_removed(tmp_work_dir):
    # Long ago there was an useless action that failed and then was rightly expunged from the study pipeline
    obsolete_action_def = """
  obsolete_action:
    run: python:latest -c pass
    outputs:
      moderately_sensitive:
        name: path.csv
    """
    create_jobs_with_project_file(
        make_job_request(action="obsolete_action"), TEST_PROJECT + obsolete_action_def
    )
    update_where(Job, {"state": State.FAILED}, action="obsolete_action")

    # Since then all the healthy, vigorous actions have been successfully run individually
    request = make_job_request(
        actions=["generate_dataset", "prepare_data_1", "prepare_data_2", "analyse_data"]
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
    generate_job = find_one(Job, action="generate_dataset")
    assert analyse_job.cancelled == 0
    assert prepare_1_job.cancelled == 1
    assert prepare_2_job.cancelled == 1
    assert generate_job.cancelled == 0


@pytest.mark.parametrize(
    "patch_config,params,exc_msg,exc_cls",
    [
        ({}, {"workspace": None}, "Workspace name cannot be blank", JobRequestError),
        ({}, {"workspace": "$%#"}, "Invalid workspace", JobRequestError),
        ({}, {"database_name": "invalid"}, "Invalid database name", JobRequestError),
        ({}, {"backend": "foo"}, "Invalid backend", JobRequestError),
        (
            {},
            {"requested_actions": []},
            "At least one action must be supplied",
            JobRequestError,
        ),
        (
            {"controller": {"ALLOWED_GITHUB_ORGS": ["test"]}},
            {"repo_url": "https://not-gihub.com/invalid"},
            "must start https://github.com",
            GithubValidationError,
        ),
        (
            {"controller": {"ALLOWED_GITHUB_ORGS": ["test"]}},
            {"repo_url": "https://github.com/test"},
            "Repository URL was not of the expected format",
            GithubValidationError,
        ),
    ],
)
def test_validate_job_request(patch_config, params, exc_msg, exc_cls, monkeypatch):
    for config_type, config_items in patch_config.items():
        for config_key, config_value in config_items.items():
            monkeypatch.setattr(f"{config_type}.config.{config_key}", config_value)
    repo_url = str(FIXTURES_PATH / "git-repo")
    kwargs = dict(
        id="123",
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit="d1e88b31cbe8f67c58f938adb5ee500d54a69764",
        branch="v1",
        requested_actions=["generate_dataset"],
        cancelled_actions=[],
        workspace="1",
        codelists_ok=True,
        database_name="default",  # note db from from job-server is 'default',
        backend="test",
        original=dict(
            created_by="user",
            project="project",
            orgs=["org1", "org2"],
        ),
    )
    kwargs.update(params)
    job_request = JobRequest(**kwargs)

    with pytest.raises(exc_cls, match=exc_msg):
        validate_job_request(job_request)


def make_job_request(action=None, actions=None, **kwargs):
    assert not (actions and action)
    if not actions:
        if action:
            actions = [action]
        else:
            actions = ["generate_dataset"]
    job_request = JobRequest(
        id=str(uuid.uuid4()),
        repo_url="https://example.com/repo.git",
        commit="abcdef0123456789",
        workspace="1",
        codelists_ok=True,
        database_name="default",
        requested_actions=actions,
        cancelled_actions=[],
        backend="test",
        original=dict(
            created_by="user",
            project="project",
            orgs=["org1", "org2"],
        ),
    )
    for key, value in kwargs.items():
        setattr(job_request, key, value)
    return job_request


def test_create_jobs_already_requested(db, tmp_work_dir):
    create_jobs_with_project_file(make_job_request(action="analyse_data"), TEST_PROJECT)

    with pytest.raises(NothingToDoError):
        create_jobs_with_project_file(
            make_job_request(action="analyse_data"), TEST_PROJECT
        )


def test_create_jobs_already_succeeded_is_rerun(db, tmp_work_dir):
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    update_where(Job, {"state": State.SUCCEEDED}, id=generate_job.id)
    update_where(Job, {"state": State.SUCCEEDED}, id=prepare_1_job.id)
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    generate_jobs = find_where(Job, action="generate_dataset")
    assert len(generate_jobs) == 1
    prepare_1_jobs = find_where(Job, action="prepare_data_1")
    assert len(prepare_1_jobs) == 2


def test_create_jobs_force_run_dependencies(db, tmp_work_dir):
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    update_where(Job, {"state": State.SUCCEEDED}, id=generate_job.id)
    update_where(Job, {"state": State.SUCCEEDED}, id=prepare_1_job.id)
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1", force_run_dependencies=True),
        TEST_PROJECT,
    )
    generate_jobs = find_where(Job, action="generate_dataset")
    assert len(generate_jobs) == 2
    prepare_1_jobs = find_where(Job, action="prepare_data_1")
    assert len(prepare_1_jobs) == 2


def test_create_jobs_reruns_failed_dependencies(db, tmp_work_dir):
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    update_where(Job, {"state": State.FAILED}, id=generate_job.id)
    update_where(Job, {"state": State.SUCCEEDED}, id=prepare_1_job.id)
    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    generate_jobs = find_where(Job, action="generate_dataset")
    assert len(generate_jobs) == 2
    prepare_1_jobs = find_where(Job, action="prepare_data_1")
    assert len(prepare_1_jobs) == 2


def create_jobs_with_project_file(job_request, project_file):
    with mock.patch("controller.create_or_update_jobs.get_project_file") as f:
        f.return_value = project_file
        return create_jobs(job_request)


def test_create_jobs_tracing(db, tmp_work_dir):
    assert count_where(Job) == 0

    create_jobs_with_project_file(
        make_job_request(action="prepare_data_1"), TEST_PROJECT
    )
    spans = get_trace("create_jobs")

    assert {span.name for span in spans} == {
        "create_jobs.load_pipeline",
        "create_jobs.get_latest_jobs",
        "create_jobs.get_new_jobs",
        "create_jobs.resolve_refs",
        "create_jobs.insert_into_database",
        "create_jobs",
    }

    assert spans[-1].name == "create_jobs"
    assert spans[-1].attributes["backend"] == "test"
    assert spans[-1].attributes["workspace"] == "1"
    assert spans[-1].attributes["len_latest_jobs"] == 0
    assert spans[-1].attributes["len_new_jobs"] == 2

    assert count_where(Job) == 2


def test_create_job_from_exception(db):
    job_request = job_request_factory_raw()

    create_job_from_exception(job_request, Exception("test"))

    job = find_one(Job, job_request_id=job_request.id)

    assert job.state == State.FAILED
    assert job.status_code == StatusCode.INTERNAL_ERROR
    assert job.status_message == "Exception: test"
    assert job.action == "__error__"

    spans = get_trace("jobs")

    assert spans[0].name == "INTERNAL_ERROR"
    assert not spans[0].status.is_ok
    assert spans[0].events[0].name == "exception"
    assert spans[0].events[0].attributes["exception.message"] == "test"
    assert spans[1].name == "JOB"
    assert not spans[1].status.is_ok
    assert spans[1].events[0].name == "exception"
    assert spans[1].events[0].attributes["exception.message"] == "test"


def test_create_job_from_exception_nothing_to_do(db):
    job_request = job_request_factory_raw()

    create_job_from_exception(job_request, NothingToDoError("nothing to do"))
    job = find_one(Job, job_request_id=job_request.id)

    assert job.state == State.SUCCEEDED
    assert job.status_code == StatusCode.SUCCEEDED
    assert job.status_message == "nothing to do"
    assert job.action == job_request.requested_actions[0]

    spans = get_trace("jobs")

    assert spans[0].name == "SUCCEEDED"
    assert spans[0].status.is_ok
    assert spans[1].name == "JOB"
    assert spans[1].status.is_ok


def test_create_job_from_exception_stale_codelist(db):
    job_request = job_request_factory_raw()

    create_job_from_exception(job_request, StaleCodelistError("stale"))
    job = find_one(Job, job_request_id=job_request.id)

    assert job.state == State.FAILED
    assert job.status_code == StatusCode.STALE_CODELISTS
    assert job.status_message == "stale"
    assert job.action == "__error__"

    spans = get_trace("jobs")

    assert spans[0].name == "STALE_CODELISTS"
    assert spans[0].status.is_ok
    assert spans[1].name == "JOB"
    assert spans[1].status.is_ok


@pytest.mark.parametrize(
    "requested_action,expect_error",
    [("generate_dataset", True), ("analyse_data", True), ("standalone_action", False)],
)
def test_create_or_update_jobs_with_out_of_date_codelists(
    tmp_work_dir, requested_action, expect_error
):
    project = TEST_PROJECT + (
        """
  standalone_action:
    run: python:latest analysis/do_something.py
    outputs:
      moderately_sensitive:
        something: done.txt
"""
    )
    job_request = make_job_request(action=requested_action, codelists_ok=False)
    if expect_error:
        # The error reports the action that needed the up-to-date codelists, even if that
        # wasn't the action explicitly requested
        with pytest.raises(
            StaleCodelistError,
            match=re.escape(
                "Codelists are out of date (required by action generate_dataset)"
            ),
        ):
            create_jobs_with_project_file(job_request, project)
    else:
        assert create_jobs_with_project_file(job_request, project) == 1
