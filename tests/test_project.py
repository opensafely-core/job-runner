import os
import tempfile
from unittest.mock import patch

import pytest

from jobrunner.exceptions import ProjectValidationError
from jobrunner.project import make_container_name, parse_project_yaml


@pytest.fixture(scope="function")
def mock_env(monkeypatch):
    monkeypatch.setenv("BACKEND", "tpp")
    monkeypatch.setenv("HIGH_PRIVACY_STORAGE_BASE", "/tmp/storage/highsecurity")
    monkeypatch.setenv("MEDIUM_PRIVACY_STORAGE_BASE", "/tmp/storage/mediumsecurity")
    monkeypatch.setenv("JOB_SERVER_ENDPOINT", "http://test.com/jobs/")


@pytest.fixture(scope="function")
def workspace():
    return {
        "repo": "https://github.com/repo",
        "db": "full",
        "owner": "me",
        "name": "tofu",
        "branch": "master",
        "id": 1,
    }


def test_bad_volume_name_is_corrected():
    bad_name = "/badname"
    assert make_container_name(bad_name) == "badname"


def test_job_to_project_nodeps(mock_env, workspace):
    """Does project information get added to a job correctly in the happy
    path?

    """
    project_path = "tests/fixtures/simple_project_1"
    job = {"operation": "generate_cohorts", "workspace": workspace}

    project = parse_project_yaml(project_path, job)
    assert project["docker_invocation"] == [
        "docker.opensafely.org/cohortextractor:0.5.2",
        "generate_cohort",
        "--output-dir=/workspace",
        "--database-url=sqlite:///test.db",
    ]
    assert project["outputs"]["highly_sensitive"]["cohort"] == "input.csv"


def test_operation_not_in_project(mock_env, workspace):
    """Do jobs whose operation is not specified in a project raise an
    exception?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = {"operation": "do_the_twist", "workspace": workspace}
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


def test_duplicate_operation_in_project(mock_env, workspace):
    """Do jobs whose operation is duplicated in a project raise an
    exception?

    """
    project_path = "tests/fixtures/invalid_project_1"
    job_spec = {"operation": "run_model_1", "workspace": workspace}
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


def test_invalid_run_in_project(mock_env, workspace):
    """Do jobs with unsupported run commands in their project raise an
    exception?

    """
    project_path = "tests/fixtures/invalid_project_2"
    job_spec = {"operation": "run_model_1", "workspace": workspace}
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


def test_valid_run_in_project(mock_env, workspace):
    """Do run commands in jobs get their variables interpolated?

    """
    project_path = "tests/fixtures/simple_project_2"
    job_spec = {"operation": "generate_cohort", "workspace": workspace}
    project = parse_project_yaml(project_path, job_spec)
    assert project["docker_invocation"] == [
        "docker.opensafely.org/cohortextractor:0.5.2",
        "generate_cohort",
        "--output-dir=/workspace",
        "--database-url=sqlite:///test.db",
    ]


@patch("jobrunner.utils.make_output_path")
def test_project_output_missing_raises_exception(
    dummy_output_path, mock_env, workspace
):
    """Do user-supplied variables that reference non-existent outputs
    raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_3"
    job_spec = {"operation": "run_model", "workspace": workspace}
    with tempfile.TemporaryDirectory() as d:
        dummy_output_path.return_value = d
        with open(os.path.join(d, "input.csv"), "w") as f:
            f.write("")
        with pytest.raises(ProjectValidationError):
            parse_project_yaml(project_path, job_spec)


@patch("jobrunner.utils.make_output_path")
def test_bad_variable_path_raises_exception(dummy_output_path, mock_env, workspace):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_4"
    job_spec = {"operation": "run_model", "workspace": workspace}
    with tempfile.TemporaryDirectory() as d:
        dummy_output_path.return_value = d
        with open(os.path.join(d, "input.csv"), "w") as f:
            f.write("")
        with pytest.raises(ProjectValidationError):
            parse_project_yaml(project_path, job_spec)


@patch("jobrunner.utils.make_output_path")
def test_bad_version_raises_exception(dummy_output_path, mock_env, workspace):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_5"
    job_spec = {"operation": "extract", "workspace": workspace}
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


@patch("jobrunner.utils.make_output_path")
def test_invalid_output_file_raises_exception(dummy_output_path, mock_env, workspace):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_6"
    job_spec = {"operation": "extract", "workspace": workspace}
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)
