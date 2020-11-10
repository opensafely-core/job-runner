import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from jobrunner import utils
from jobrunner.exceptions import ProjectValidationError
from jobrunner.project import make_container_name, parse_project_yaml, validate_project


def test_bad_volume_name_is_corrected():
    bad_name = "/badname"
    assert make_container_name(bad_name) == "badname"


def test_job_to_project_nodeps(job_spec_maker):
    """Does project information get added to a job correctly in the happy
    path?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = job_spec_maker(action_id="generate_cohorts")

    project = parse_project_yaml(project_path, job_spec)
    assert project["docker_invocation"] == [
        "-e",
        "DATABASE_URL=sqlite:///test.db",
        "-e",
        "TEMP_DATABASE_NAME=temp",
        "docker.opensafely.org/cohortextractor:0.5.2",
        "generate_cohort",
        f"--output-dir={utils.get_workdir()}",
    ]
    assert project["outputs"]["highly_sensitive"]["cohort"] == "input.csv"


def test_job_to_project_with_deps(job_spec_maker):
    """Does project information get added to a job correctly in the happy path?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = job_spec_maker(action_id="run_model")

    project = parse_project_yaml(project_path, job_spec)
    assert "generate_cohorts" in project["dependencies"]
    dependency = project["dependencies"]["generate_cohorts"]
    assert (
        dependency["workspace"] == project["workspace"]
    ), "Dependency must be in the same workspace as called action"


def test_valid_run_in_project(job_spec_maker):
    """Do run commands in jobs get their variables interpolated?

    """
    project_path = "tests/fixtures/simple_project_2"
    job_spec = job_spec_maker(action_id="generate_cohort")
    project = parse_project_yaml(project_path, job_spec)
    assert project["docker_invocation"] == [
        "-e",
        "DATABASE_URL=sqlite:///test.db",
        "-e",
        "TEMP_DATABASE_NAME=temp",
        "docker.opensafely.org/cohortextractor:0.5.2",
        "generate_cohort",
        f"--output-dir={utils.get_workdir()}",
    ]


def test_action_id_not_in_project(job_spec_maker):
    """Do jobs whose action_id is not specified in a project raise an
    exception?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = job_spec_maker(action_id="do_the_twist")
    with pytest.raises(ProjectValidationError):
        parse_project_yaml(project_path, job_spec)


@patch("jobrunner.utils.get_workdir")
@patch("jobrunner.utils.all_output_paths_for_action")
def test_project_needs_run(dummy_output_paths, mock_get_workdir, job_spec_maker):
    """Do complete dependencies with force_run set raise an exception?
    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = job_spec_maker(action_id="run_model")

    # Check using output paths that don't exist, so run is needed
    dummy_output_paths.return_value = [
        {"base_path": "", "namespace": "", "relative_path": "test-xxx"}
    ]
    mock_get_workdir.return_value = ""
    parsed = parse_project_yaml(project_path, job_spec)
    assert parsed["needs_run"] is True

    # Check using output paths that do exist, so run not needed unless
    # explicitly asked
    with tempfile.TemporaryDirectory() as d:
        mock_get_workdir.return_value = d
        mock_output_filename = os.path.join(d, "input.csv")
        with open(mock_output_filename, "w") as f:
            f.write("")
        dummy_output_paths.return_value = [
            {"base_path": "", "namespace": "", "relative_path": mock_output_filename}
        ]

        parsed = parse_project_yaml(project_path, job_spec)
        assert parsed["needs_run"] is False

        job_spec["force_run"] = True
        parsed = parse_project_yaml(project_path, job_spec)
        assert parsed["needs_run"] is True
        assert parsed["dependencies"]["generate_cohorts"]["needs_run"] is False

        job_spec["force_run_dependencies"] = True
        parsed = parse_project_yaml(project_path, job_spec)
        assert parsed["needs_run"] is True
        assert parsed["dependencies"]["generate_cohorts"]["needs_run"] is True


def test_duplicate_action_id_in_project():
    """Do jobs whose action_id is duplicated in a project raise an
    exception?

    """
    project_path = "tests/fixtures/invalid_project_1"

    with open(Path(project_path) / "project.yaml", "r") as f:
        project = yaml.safe_load(f)

    with pytest.raises(ProjectValidationError, match="appears more than once"):
        validate_project(project_path, project)


def test_invalid_run_in_project():
    """Do jobs with unsupported run commands in their project raise an
    exception?

    """
    project_path = "tests/fixtures/invalid_project_2"

    with open(Path(project_path) / "project.yaml", "r") as f:
        project = yaml.safe_load(f)

    with pytest.raises(ProjectValidationError, match="not a supported command"):
        validate_project(project_path, project)


def test_project_output_missing_raises_exception():
    """Do user-supplied variables that reference non-existent outputs
    raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_3"

    with open(Path(project_path) / "project.yaml", "r") as f:
        project = yaml.safe_load(f)

    with pytest.raises(ProjectValidationError, match="Unable to find variable"):
        validate_project(project_path, project)


def test_bad_variable_path_raises_exception():
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_4"

    with open(Path(project_path) / "project.yaml", "r") as f:
        project = yaml.safe_load(f)

    with pytest.raises(ProjectValidationError, match="Unable to find variable"):
        validate_project(project_path, project)


def test_bad_version_raises_exception():
    """Do run commands without version numbers specified raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_5"

    with open(Path(project_path) / "project.yaml", "r") as f:
        project = yaml.safe_load(f)

    with pytest.raises(ProjectValidationError, match="must have a version specified"):
        validate_project(project_path, project)


def test_invalid_output_file_raises_exception():
    """Do output files that have unsafe directory traversal raise an exception?

    """
    project_path = "tests/fixtures/invalid_project_6"

    with open(Path(project_path) / "project.yaml", "r") as f:
        project = yaml.safe_load(f)

    with pytest.raises(ProjectValidationError, match="is not permitted"):
        validate_project(project_path, project)


def test_duplicate_output_file_raises_exception():
    """Do jobs whose action_id is duplicated in a project raise an
    exception?

    """
    project_path = "tests/fixtures/invalid_project_7"
    with open(Path(project_path) / "project.yaml", "r") as f:
        project = yaml.safe_load(f)

    with pytest.raises(ProjectValidationError, match="is not unique"):
        validate_project(project_path, project)


def test_missing_expectations_raises_exception():
    """Do projects of version 3 or more require an expectations section?
    """
    project_path = "tests/fixtures/invalid_project_8"
    with open(Path(project_path) / "project.yaml", "r") as f:
        project = yaml.safe_load(f)

    with pytest.raises(ProjectValidationError, match="must include `expectations`"):
        validate_project(project_path, project)


def test_old_versions_supply_default_popuation():
    """Do projects of version < 3 supply default expectations population?
    """
    project_path = "tests/fixtures/simple_project_1"
    with open(Path(project_path) / "project.yaml", "r") as f:
        project = yaml.safe_load(f)
    project = validate_project(project_path, project)
    assert project["expectations"]["population_size"] == 1000
