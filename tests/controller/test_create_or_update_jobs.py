import re
import uuid
from pathlib import Path
from unittest import mock

import pytest

import common.config
import controller
from common.lib.github_validators import GithubValidationError
from controller.create_or_update_jobs import (
    NothingToDoError,
    RapCreateRequestError,
    StaleCodelistError,
    create_jobs,
    validate_rap_create_request,
)
from controller.lib.database import count_where, find_one, find_where, update_where
from controller.models import Job, State
from controller.webapp.views.validators.dataclasses import CreateRequest
from tests.conftest import get_trace


FIXTURES_PATH = Path(__file__).parent.parent.resolve() / "fixtures"


@pytest.fixture()
def disable_github_org_checking(monkeypatch):
    monkeypatch.setattr("common.config.ALLOWED_GITHUB_ORGS", None)


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
    create_jobs_with_project_file(
        make_create_request(action="analyse_data"), TEST_PROJECT
    )
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
        make_create_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    assert prepare_1_job.wait_for_job_ids == [generate_job.id]
    # Now schedule a job which has the above jobs as dependencies
    create_jobs_with_project_file(
        make_create_request(action="analyse_data"), TEST_PROJECT
    )
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
        make_create_request(action="analyse_data", backend="foo"), TEST_PROJECT
    )
    create_jobs_with_project_file(
        make_create_request(action="analyse_data", backend="bar"), TEST_PROJECT
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
        make_create_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    assert prepare_1_job.wait_for_job_ids == [generate_job.id]
    # make the generate_job succeeded
    update_where(Job, {"state": State.SUCCEEDED}, id=generate_job.id)

    # Now schedule a job which has the above jobs as dependencies
    create_jobs_with_project_file(
        make_create_request(action="analyse_data"), TEST_PROJECT
    )
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
        make_create_request(action="generate_dataset"), TEST_PROJECT
    )
    cancelled_generate_job = find_one(Job, action="generate_dataset")
    update_where(Job, {"cancelled": True}, id=cancelled_generate_job.id)

    # Now schedule a job which has the above job as a dependency
    create_jobs_with_project_file(
        make_create_request(action="prepare_data_1"), TEST_PROJECT
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
        make_create_request(action="obsolete_action"),
        TEST_PROJECT + obsolete_action_def,
    )
    update_where(Job, {"state": State.FAILED}, action="obsolete_action")

    # Since then all the healthy, vigorous actions have been successfully run individually
    request = make_create_request(
        actions=["generate_dataset", "prepare_data_1", "prepare_data_2", "analyse_data"]
    )
    create_jobs_with_project_file(request, TEST_PROJECT)
    update_where(Job, {"state": State.SUCCEEDED}, rap_id=request.id)

    with pytest.raises(NothingToDoError):
        # Now this should be a no-op because all the actions that are still part of the study have succeeded
        create_jobs_with_project_file(
            make_create_request(action="run_all"), TEST_PROJECT
        )


@pytest.mark.parametrize(
    "params,exc_msg,exc_cls",
    [
        ({"workspace": None}, "Workspace name cannot be blank", RapCreateRequestError),
        ({"workspace": "$%#"}, "Invalid workspace", RapCreateRequestError),
        ({"database_name": "invalid"}, "Invalid database name", RapCreateRequestError),
        ({"backend": "foo"}, "Invalid backend", RapCreateRequestError),
        (
            {"requested_actions": []},
            "At least one action must be supplied",
            RapCreateRequestError,
        ),
    ],
)
def test_validate_rap_create_request(params, exc_msg, exc_cls, monkeypatch):
    repo_url = str(FIXTURES_PATH / "git-repo")
    kwargs = dict(
        id="123",
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit="d1e88b31cbe8f67c58f938adb5ee500d54a69764",
        branch="v1",
        requested_actions=["generate_dataset"],
        workspace="1",
        codelists_ok=True,
        database_name="default",  # note db from from job-server is 'default',
        backend="test",
        force_run_dependencies=False,
        created_by="",
        project="",
        orgs=[],
        analysis_scope={},
    )
    kwargs.update(params)
    rap_create_request = CreateRequest(**kwargs)

    with pytest.raises(exc_cls, match=exc_msg):
        validate_rap_create_request(rap_create_request)


@pytest.mark.parametrize(
    "repo_url,exc_msg,exc_cls",
    [
        (
            "https://not-gihub.com/invalid",
            "does not start with https://github.com",
            GithubValidationError,
        ),
        (
            "https://github.com/test",
            "not of the expected format",
            GithubValidationError,
        ),
    ],
)
def test_validate_rap_create_request_repos(repo_url, exc_msg, exc_cls, monkeypatch):
    monkeypatch.setattr(common.config, "ALLOWED_GITHUB_ORGS", ["test"])
    kwargs = dict(
        id="123",
        repo_url=repo_url,
        commit="d1e88b31cbe8f67c58f938adb5ee500d54a69764",
        branch="v1",
        requested_actions=["generate_dataset"],
        workspace="1",
        codelists_ok=True,
        database_name="default",  # note db from from job-server is 'default',
        backend="test",
        force_run_dependencies=False,
        created_by="",
        project="",
        orgs=[],
        analysis_scope={},
    )
    rap_create_request = CreateRequest(**kwargs)

    with pytest.raises(exc_cls, match=exc_msg):
        validate_rap_create_request(rap_create_request)


def make_create_request(action=None, actions=None, **kwargs):
    assert not (actions and action)
    if not actions:
        if action:
            actions = [action]
        else:
            actions = ["generate_dataset"]
    rap_create_request = CreateRequest(
        id=str(uuid.uuid4()),
        # do not use a http url so we bypass repo validation
        repo_url="/some/url/repo",
        commit="abcdef0123456789",
        workspace="1",
        codelists_ok=True,
        database_name="default",
        requested_actions=actions,
        backend="test",
        branch="main",
        force_run_dependencies=False,
        created_by="testuser",
        project="project",
        orgs=["org1", "org2"],
        analysis_scope={},
    )
    for key, value in kwargs.items():
        setattr(rap_create_request, key, value)
    return rap_create_request


def test_create_jobs_already_requested(db, tmp_work_dir):
    create_jobs_with_project_file(
        make_create_request(action="analyse_data"), TEST_PROJECT
    )

    with pytest.raises(NothingToDoError):
        create_jobs_with_project_file(
            make_create_request(action="analyse_data"), TEST_PROJECT
        )


def test_create_jobs_already_succeeded_is_rerun(db, tmp_work_dir):
    create_jobs_with_project_file(
        make_create_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    update_where(Job, {"state": State.SUCCEEDED}, id=generate_job.id)
    update_where(Job, {"state": State.SUCCEEDED}, id=prepare_1_job.id)
    create_jobs_with_project_file(
        make_create_request(action="prepare_data_1"), TEST_PROJECT
    )
    generate_jobs = find_where(Job, action="generate_dataset")
    assert len(generate_jobs) == 1
    prepare_1_jobs = find_where(Job, action="prepare_data_1")
    assert len(prepare_1_jobs) == 2


def test_create_jobs_force_run_dependencies(db, tmp_work_dir):
    create_jobs_with_project_file(
        make_create_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    update_where(Job, {"state": State.SUCCEEDED}, id=generate_job.id)
    update_where(Job, {"state": State.SUCCEEDED}, id=prepare_1_job.id)
    create_jobs_with_project_file(
        make_create_request(action="prepare_data_1", force_run_dependencies=True),
        TEST_PROJECT,
    )
    generate_jobs = find_where(Job, action="generate_dataset")
    assert len(generate_jobs) == 2
    prepare_1_jobs = find_where(Job, action="prepare_data_1")
    assert len(prepare_1_jobs) == 2


def test_create_jobs_reruns_failed_dependencies(db, tmp_work_dir):
    create_jobs_with_project_file(
        make_create_request(action="prepare_data_1"), TEST_PROJECT
    )
    prepare_1_job = find_one(Job, action="prepare_data_1")
    generate_job = find_one(Job, action="generate_dataset")
    update_where(Job, {"state": State.FAILED}, id=generate_job.id)
    update_where(Job, {"state": State.SUCCEEDED}, id=prepare_1_job.id)
    create_jobs_with_project_file(
        make_create_request(action="prepare_data_1"), TEST_PROJECT
    )
    generate_jobs = find_where(Job, action="generate_dataset")
    assert len(generate_jobs) == 2
    prepare_1_jobs = find_where(Job, action="prepare_data_1")
    assert len(prepare_1_jobs) == 2


def create_jobs_with_project_file(rap_create_request, project_file):
    with mock.patch("controller.create_or_update_jobs.get_project_file") as f:
        f.return_value = project_file
        return create_jobs(rap_create_request)


def test_create_jobs_tracing(db, tmp_work_dir):
    assert count_where(Job) == 0

    with mock.patch.object(
        CreateRequest,
        "get_tracing_span_attributes",
        return_value={"foo": "bar", "orgs": ["o1", "o2"]},
    ):
        create_jobs_with_project_file(
            make_create_request(action="prepare_data_1"), TEST_PROJECT
        )
    spans = get_trace("create_or_update_jobs")

    assert {span.name for span in spans} == {
        "create_jobs",
    }

    assert spans[0].name == "create_jobs"
    assert spans[0].attributes["foo"] == "bar"  # patched
    assert spans[0].attributes["orgs"] == ("o1", "o2")  # patched

    assert count_where(Job) == 2


@pytest.mark.parametrize(
    "requested_action,expect_error",
    [("generate_dataset", True), ("analyse_data", True), ("standalone_action", False)],
)
def test_create_jobs_with_out_of_date_codelists(
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
    rap_create_request = make_create_request(
        action=requested_action, codelists_ok=False
    )
    if expect_error:
        # The error reports the action that needed the up-to-date codelists, even if that
        # wasn't the action explicitly requested
        with pytest.raises(
            StaleCodelistError,
            match=re.escape(
                "Codelists are out of date (required by action generate_dataset)"
            ),
        ):
            create_jobs_with_project_file(rap_create_request, project)
    else:
        assert create_jobs_with_project_file(rap_create_request, project) == 1


def test_create_non_db_job_with_analysis_scope(tmp_work_dir):
    create_rap_request = make_create_request(action="analyse_data")
    create_jobs_with_project_file(create_rap_request, TEST_PROJECT)
    job = find_one(Job, action="analyse_data")
    assert job.analysis_scope == {}


@pytest.mark.parametrize(
    "rap_api_analysis_scope,project,expected_job_analysis_scope",
    [
        (None, "project-with-no-permissions", {"dataset_permissions": []}),
        ({}, "project-with-no-permissions", {"dataset_permissions": []}),
        ({}, "project-with-permissions", {"dataset_permissions": ["table1", "table2"]}),
        (
            {"dataset_permissions": ["table3"]},
            "project-with-permissions",
            {"dataset_permissions": ["table1", "table2", "table3"]},
        ),
    ],
)
def test_create_db_job_with_dataset_permissions(
    tmp_work_dir,
    monkeypatch,
    rap_api_analysis_scope,
    project,
    expected_job_analysis_scope,
):
    monkeypatch.setattr(
        controller.permissions.datasets,
        "PERMISSIONS",
        {
            "project-with-permissions": ["table1", "table2"],
        },
    )
    create_rap_request = make_create_request(
        action="generate_dataset",
        analysis_scope=rap_api_analysis_scope,
        project=project,
    )
    create_jobs_with_project_file(create_rap_request, TEST_PROJECT)
    job = find_one(Job, action="generate_dataset")

    assert job.analysis_scope == expected_job_analysis_scope


@pytest.mark.parametrize(
    "rap_api_analysis_scope,repo_url,expected_job_analysis_scope",
    [
        ({}, "https://github.com/opensafely/not-ok-repo", {"dataset_permissions": []}),
        (
            {},
            "https://github.com/opensafely/ok-repo",
            {"dataset_permissions": [], "component_access": ["event_level_data"]},
        ),
        (
            {"component_access": ["some_other_component"]},
            "https://github.com/opensafely/ok-repo",
            {
                "dataset_permissions": [],
                "component_access": ["event_level_data", "some_other_component"],
            },
        ),
    ],
)
def test_create_db_job_with_component_access(
    tmp_work_dir,
    monkeypatch,
    rap_api_analysis_scope,
    repo_url,
    expected_job_analysis_scope,
):
    monkeypatch.setattr(
        controller.config,
        "REPOS_WITH_EHRQL_EVENT_LEVEL_ACCESS",
        {"https://github.com/opensafely/ok-repo"},
    )
    create_rap_request = make_create_request(
        action="generate_dataset",
        analysis_scope=rap_api_analysis_scope,
        repo_url=repo_url,
    )
    with mock.patch("controller.create_or_update_jobs.validate_rap_create_request"):
        create_jobs_with_project_file(create_rap_request, TEST_PROJECT)
    job = find_one(Job, action="generate_dataset")

    assert job.analysis_scope == expected_job_analysis_scope
