import argparse
import shlex

import pytest

from jobrunner import project
from jobrunner.project import (
    InvalidPatternError,
    ProjectValidationError,
    assert_valid_glob_pattern,
    parse_and_validate_project_file,
)


class TestParseAndValidateProjectFile:
    def test_with_action(self):
        project_file = """
        version: '3.0'
        expectations:
            population_size: 1000
        actions:
            my_action:
                run: python:latest python analysis/my_action.py
                outputs:
                    moderately_sensitive:
                        my_figure: output/my_figure.png
        """
        project = parse_and_validate_project_file(project_file)
        obs_run = project["actions"]["my_action"]["run"]
        exp_run = "python:latest python analysis/my_action.py"
        assert obs_run == exp_run

    def test_with_duplicate_keys(self):
        project_file = """
            top_level:
                duplicate: 1
                duplicate: 2
        """
        with pytest.raises(ProjectValidationError):
            parse_and_validate_project_file(project_file)


class TestAddConfigToRunCommand:
    def test_with_option(self):
        run_command = "python:latest python analysis/my_action.py --option value"
        config = {"option": "value"}
        obs_run_command = project.add_config_to_run_command(run_command, config)
        exp_run_command = """python:latest python analysis/my_action.py --option value --config '{"option": "value"}'"""
        assert obs_run_command == exp_run_command

    def test_with_argument(self):
        run_command = "python:latest python action/__main__.py output/input.csv"
        config = {"option": "value"}
        obs_run_command = project.add_config_to_run_command(run_command, config)
        exp_run_command = """python:latest python action/__main__.py output/input.csv --config '{"option": "value"}'"""
        assert obs_run_command == exp_run_command

        # Does argparse accept options after arguments?
        parser = argparse.ArgumentParser()
        parser.add_argument("--config")  # option
        parser.add_argument("input_files", nargs="*")  # argument
        # If parser were in __main__.py, then parser.parse_args would receive sys.argv
        # by default. sys.argv[0] is the script name (either with or without a path,
        # depending on the OS) so we slice obs_run_command to mimic this.
        parser.parse_args(shlex.split(obs_run_command)[2:])


def test_assert_valid_glob_pattern():
    assert_valid_glob_pattern("foo/bar/*.txt")
    assert_valid_glob_pattern("foo")
    bad_patterns = [
        "/abs/path",
        "ends/in/slash/",
        "not//canonical",
        "path/../traversal",
        "c:/windows/absolute",
        "recursive/**/glob.pattern",
        "questionmark?",
        "/[square]brackets",
    ]
    for pattern in bad_patterns:
        with pytest.raises(InvalidPatternError):
            assert_valid_glob_pattern(pattern)


def test_get_action_specification_with_unknown_action():
    project_dict = {"actions": {"known_action": {}}}
    action_id = "unknown_action"
    with pytest.raises(project.UnknownActionError):
        project.get_action_specification(project_dict, action_id)


def test_get_action_specification_with_config():
    project_dict = {
        "actions": {
            "my_action": {
                "run": "python:latest python analysis/my_action.py",
                "config": {"my_key": "my_value"},
                "outputs": {
                    "moderately_sensitive": {"my_figure": "output/my_figure.png"}
                },
            }
        }
    }
    action_id = "my_action"
    action_spec = project.get_action_specification(project_dict, action_id)
    assert (
        action_spec.run
        == """python:latest python analysis/my_action.py --config '{"my_key": "my_value"}'"""
    )


def test_get_action_specification_for_cohortextractor_generate_cohort_action():
    project_dict = {
        "expectations": {"population_size": 1_000},
        "actions": {
            "generate_cohort": {
                "run": "cohortextractor:latest generate_cohort",
                "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
            }
        },
    }
    action_id = "generate_cohort"
    action_spec = project.get_action_specification(project_dict, action_id)
    assert (
        action_spec.run
        == """cohortextractor:latest generate_cohort --expectations-population=1000 --output-dir=output"""
    )


@pytest.mark.parametrize("image", ["cohortextractor-v2", "databuilder"])
def test_get_action_specification_for_databuilder_action(image):
    project_dict = {
        "expectations": {"population_size": 1_000},
        "actions": {
            "generate_cohort_v2": {
                "run": f"{image}:latest generate_cohort --output=output/cohort.csv --dummy-data-file dummy.csv",
                "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
            }
        },
    }
    action_id = "generate_cohort_v2"
    action_spec = project.get_action_specification(project_dict, action_id)
    assert (
        action_spec.run
        == f"""{image}:latest generate_cohort --output=output/cohort.csv --dummy-data-file dummy.csv"""
    )


@pytest.mark.parametrize(
    "args,error,image",
    [
        (
            "--output=output/cohort1.csv --dummy-data-file dummy.csv",
            "--output in run command and outputs must match",
            "cohortextractor-v2",
        ),
        (
            "--output=output/cohort1.csv",
            "--dummy-data-file is required for a local run",
            "cohortextractor-v2",
        ),
        (
            "--output=output/cohort1.csv --dummy-data-file dummy.csv",
            "--output in run command and outputs must match",
            "databuilder",
        ),
        (
            "--output=output/cohort1.csv",
            "--dummy-data-file is required for a local run",
            "databuilder",
        ),
    ],
)
def test_get_action_specification_for_databuilder_errors(args, error, image):
    project_dict = {
        "expectations": {"population_size": 1_000},
        "actions": {
            "generate_cohort_v2": {
                "run": f"{image}:latest generate_cohort {args}",
                "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
            }
        },
    }
    action_id = "generate_cohort_v2"
    with pytest.raises(ProjectValidationError, match=error):
        project.get_action_specification(project_dict, action_id)
