from unittest.mock import patch
import os
import subprocess
import tempfile

import pytest
import requests_mock

from runner.project import docker_container_exists
from runner.project import make_container_name
from runner.utils import make_volume_name
from runner.project import parse_project_yaml
from runner.exceptions import DependencyNotFinished
from runner.exceptions import ProjectValidationError
from tests.common import default_job, test_job_list


@pytest.fixture(scope="function")
def mock_env(monkeypatch):
    monkeypatch.setenv("BACKEND", "tpp")
    monkeypatch.setenv("HIGH_PRIVACY_STORAGE_BASE", "/tmp/storage/highsecurity")
    monkeypatch.setenv("MEDIUM_PRIVACY_STORAGE_BASE", "/tmp/storage/mediumsecurity")
    monkeypatch.setenv("JOB_SERVER_ENDPOINT", "http://test.com/jobs/")


def test_make_volume_name():
    repo = "https://github.com/opensafely/hiv-research/"
    branch = "feasibility-no"
    db_flavour = "full"
    assert (
        make_volume_name(repo, branch, db_flavour) == "hiv-research-feasibility-no-full"
    )


def test_bad_volume_name_is_corrected():
    bad_name = "/badname"
    assert make_container_name(bad_name) == "badname"


def test_job_to_project_nodeps(mock_env):
    """Does project information get added to a job correctly in the happy
    path?

    """
    project_path = "tests/fixtures/simple_project_1"
    job = {
        "operation": "generate_cohorts",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }

    project = parse_project_yaml(project_path, job)
    assert project["docker_invocation"] == [
        "docker.opensafely.org/cohort-extractor:0.5.2",
        "generate_cohort",
        "--output-dir=/workspace",
        "--database-url=sqlite:///test.db",
    ]
    assert project["outputs"]["highly_sensitive"]["cohort"] == "input.csv"


def test_never_started_dependency_exception(mock_env):
    """Does a never-run dependency mean an exception is raised and the
    dependency is kicked off?

    """
    project_path = "tests/fixtures/simple_project_1"
    job = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json={"results": []})
        adapter = m.post("/jobs/")
        with pytest.raises(
            DependencyNotFinished,
            match=r"Not started because dependency `generate_cohorts` has been added to the job queue",
        ):
            parse_project_yaml(project_path, job)
    assert adapter.request_history[0].json() == {
        "backend": "tpp",
        "callback_url": None,
        "db": "full",
        "needed_by": "run_model",
        "operation": "generate_cohorts",
        "repo": "https://github.com/repo",
        "tag": "master",
    }


def test_unstarted_dependency_exception(mock_env):
    """Does a existing, but unstarted dependency mean an exception is raised?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    existing_unstarted_job = default_job.copy()
    existing_unstarted_job.update(job_spec)
    existing_unstarted_job["started"] = False
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list(job=existing_unstarted_job))
        with pytest.raises(
            DependencyNotFinished,
            match=r"Not started because dependency `generate_cohorts` is waiting to start",
        ):
            parse_project_yaml(project_path, job_spec)


def test_failed_dependency_exception(mock_env):
    """Does a existing, but failed dependency mean an exception is raised?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_requested = default_job.copy()
    job_requested.update(
        {
            "operation": "run_model",
            "repo": "https://github.com/repo",
            "db": "full",
            "tag": "master",
            "workdir": "/workspace",
        }
    )
    existing_failed_job = job_requested.copy()
    existing_failed_job["started"] = True
    existing_failed_job["completed_at"] = "2020-01-01"
    existing_failed_job["status_code"] = 1
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list(job=existing_failed_job))
        with pytest.raises(
            DependencyNotFinished,
            match=r"Dependency `generate_cohorts` failed, so unable to run this operation",
        ):
            parse_project_yaml(project_path, job_requested)


@patch("runner.project.docker_container_exists")
def test_started_dependency_exception(mock_container_exists, mock_env):
    """Does an already-running dependency mean an exception is raised?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json={"results": []})
        mock_container_exists.return_value = True
        with pytest.raises(
            DependencyNotFinished,
            match=r"Not started because dependency `generate_cohorts` is currently running",
        ):
            parse_project_yaml(project_path, job_spec)


@patch("runner.utils.make_output_path")
def test_project_dependency_no_exception(dummy_output_path, mock_env):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with tempfile.TemporaryDirectory() as d:
        mock_output_filename = os.path.join(d, "input.csv")
        dummy_output_path.return_value = mock_output_filename
        with open(mock_output_filename, "w") as f:
            f.write("")
        project = parse_project_yaml(project_path, job_spec)
        assert project["docker_invocation"] == [
            "docker.opensafely.org/stata-mp:1.0",
            "analysis/model.do",
            f"generate_cohorts_input.csv",
        ]
        assert project["outputs"]["moderately_sensitive"]["log"] == "model.log"


def test_operation_not_in_project(mock_env):
    """Do jobs whose operation is not specified in a project raise an
    exception?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = {
        "operation": "do_the_twist",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
    }
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


def test_duplicate_operation_in_project(mock_env):
    """Do jobs whose operation is duplicated in a project raise an
    exception?

    """
    project_path = "tests/fixtures/invalid_project_1"
    job_spec = {
        "operation": "run_model_1",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


def test_invalid_run_in_project(mock_env):
    """Do jobs with unsupported run commands in their project raise an
    exception?

    """
    project_path = "tests/fixtures/invalid_project_2"
    job_spec = {
        "operation": "run_model_1",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


def test_valid_run_in_project(mock_env):
    """Do run commands in jobs get their variables interpolated?

    """
    project_path = "tests/fixtures/simple_project_2"
    job_spec = {
        "operation": "generate_cohort",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    project = parse_project_yaml(project_path, job_spec)
    assert project["docker_invocation"] == [
        "docker.opensafely.org/cohort-extractor:0.5.2",
        "generate_cohort",
        "--output-dir=/workspace",
        "--database-url=sqlite:///test.db",
    ]


@patch("runner.utils.make_output_path")
def test_project_output_missing_raises_exception(dummy_output_path, mock_env):
    """Do user-supplied variables that reference non-existent outputs
    raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_3"
    job_spec = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with tempfile.TemporaryDirectory() as d:
        dummy_output_path.return_value = d
        with open(os.path.join(d, "input.csv"), "w") as f:
            f.write("")
        with pytest.raises(ProjectValidationError):
            parse_project_yaml(project_path, job_spec)


@patch("runner.utils.make_output_path")
def test_bad_variable_path_raises_exception(dummy_output_path, mock_env):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_4"
    job_spec = {
        "operation": "run_model",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with tempfile.TemporaryDirectory() as d:
        dummy_output_path.return_value = d
        with open(os.path.join(d, "input.csv"), "w") as f:
            f.write("")
        with pytest.raises(ProjectValidationError):
            parse_project_yaml(project_path, job_spec)


@patch("runner.utils.make_output_path")
def test_bad_version_raises_exception(dummy_output_path, mock_env):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_5"
    job_spec = {
        "operation": "extract",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


@patch("runner.utils.make_output_path")
def test_invalid_output_file_raises_exception(dummy_output_path, mock_env):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_6"
    job_spec = {
        "operation": "extract",
        "repo": "https://github.com/repo",
        "db": "full",
        "tag": "master",
        "workdir": "/workspace",
    }
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


def xtest_job_runner_docker_container_exists(mock_env):
    """Tests the ability to see if a container is running or not.

    This test is slow: it depends on a docker install and network
    access, and the teardown in the last line blocks for a few seconds

    """
    assert not docker_container_exists("nonexistent_container_name")

    # Start a trivial docker container
    name = "existent_container_name"
    subprocess.check_call(
        ["docker", "run", "--detach", "--rm", "--name", name, "alpine", "sleep", "60"],
    )
    assert docker_container_exists(name)
    subprocess.check_call(["docker", "stop", name])
