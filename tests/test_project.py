import argparse
import shlex
from unittest import mock

import pytest

from jobrunner import git, project
from jobrunner.project import (
    InvalidPatternError,
    ProjectValidationError,
    ReusableAction,
    assert_valid_glob_pattern,
    parse_and_validate_project_file,
)


@mock.patch.multiple(
    "jobrunner.git",
    get_sha_from_remote_ref=mock.DEFAULT,
    read_file_from_repo=mock.DEFAULT,
)
class TestHandleReusableAction:
    def test_when_not_a_reusable_action(self, **kwargs):
        # Happy path 1
        action_in = {"run": "python:latest python analysis/my_action.py"}
        action_out = project.handle_reusable_action("my_action", action_in)
        assert action_in is action_out
        kwargs["get_sha_from_remote_ref"].assert_not_called()
        kwargs["read_file_from_repo"].assert_not_called()

    @mock.patch(
        "jobrunner.project.parse_yaml_file",
        return_value={"run": "python:latest python reusable_action/main.py"},
    )
    def test_when_a_reusable_action_with_options(self, *args, **kwargs):
        # Happy path 2
        action_in = {"run": "reusable-action:latest --output-format=png"}
        action_out = project.handle_reusable_action("my_action", action_in)
        assert action_in is not action_out
        assert (
            action_out["run"]
            == "python:latest python reusable_action/main.py --output-format=png"
        )

    @mock.patch(
        "jobrunner.project.parse_yaml_file",
        return_value={"run": "python:latest python reusable_action/main.py"},
    )
    def test_when_a_reusable_action_with_arguments(self, *args, **kwargs):
        # Happy path 3
        action_in = {"run": "reusable-action:latest output/input.csv"}
        action_out = project.handle_reusable_action("my_action", action_in)
        assert action_in is not action_out
        assert (
            action_out["run"]
            == "python:latest python reusable_action/main.py output/input.csv"
        )

    @mock.patch(
        "jobrunner.project.parse_yaml_file",
        return_value={"run": "python:latest python reusable_action/main.py"},
    )
    def test_when_a_reusable_action_with_options_and_arguments(self, *args, **kwargs):
        # Happy path 4
        # We'll use Click's terminology.
        # * Options are optional
        # * Arguments are optional within reason, but are more restricted than options
        # For more information, see:
        # https://click.palletsprojects.com/en/8.0.x/parameters/
        action_in = {
            "run": "reusable-action:latest --output-format=png output/input.csv"
        }
        action_out = project.handle_reusable_action("my_action", action_in)
        assert action_in is not action_out
        assert (
            action_out["run"]
            == "python:latest python reusable_action/main.py --output-format=png output/input.csv"
        )

    def test_with_bad_run_command(self, **kwargs):
        # We don't need to check the scheme, netloc, or org because we add those.
        with pytest.raises(project.ReusableActionError):
            project.handle_reusable_action(
                "my_action",
                {"run": "../my-bad-org/reusable-action:latest"},
            )

    def test_with_bad_remote_ref(self, **kwargs):
        kwargs["get_sha_from_remote_ref"].side_effect = git.GitError
        with pytest.raises(project.ReusableActionError):
            project.handle_reusable_action(
                "my_action", {"run": "reusable-action:latest"}
            )

    @mock.patch(
        "jobrunner.project.validate_branch_and_commit",
        side_effect=project.GithubValidationError,
    )
    def test_with_bad_commit(self, *args, **kwargs):
        with pytest.raises(project.ReusableActionError):
            project.handle_reusable_action(
                "my_action",
                {"run": "reusable-action:latest"},
            )

    def test_with_bad_file(self, **kwargs):
        kwargs["read_file_from_repo"].side_effect = git.GitError
        with pytest.raises(project.ReusableActionError):
            project.handle_reusable_action(
                "my_action", {"run": "reusable-action:latest"}
            )

    @mock.patch(
        "jobrunner.project.parse_yaml_file",
        side_effect=project.ProjectYAMLError,
    )
    def test_with_bad_yaml(self, *args, **kwargs):
        with pytest.raises(project.ReusableActionError):
            project.handle_reusable_action(
                "my_action", {"run": "reusable-action:latest"}
            )

    @mock.patch("jobrunner.project.parse_yaml_file", return_value={})
    def test_with_bad_action_config(self, *args, **kwargs):
        with pytest.raises(project.ReusableActionError):
            project.handle_reusable_action(
                "my_action", {"run": "reusable-action:latest"}
            )

    def test_reusable_action_with_invalid_runtime(self, *args, **kwargs):
        action_id = "my_action"
        action = {"run": "foo:v1"}
        reusable_action_1 = ReusableAction(
            repo_url="foo", commit="bar", action_file=b"run: notanaction:v1"
        )
        with pytest.raises(project.ReusableActionError):
            project.apply_reusable_action(action_id, action, reusable_action_1)
        # This is a valid runtime, but it's not allowed in re-usable actions
        reusable_action_2 = ReusableAction(
            repo_url="foo",
            commit="bar",
            action_file=b"run: cohortextractor:v1 generate_cohort",
        )
        with pytest.raises(project.ReusableActionError):
            project.apply_reusable_action(action_id, action, reusable_action_2)


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

    @mock.patch.multiple(
        "jobrunner.git",
        get_sha_from_remote_ref=mock.DEFAULT,
        read_file_from_repo=mock.DEFAULT,
    )
    def test_with_reusable_action(self, **kwargs):
        project_file = """
        version: '3.0'
        expectations:
            population_size: 1000
        actions:
            my_action:
                run: reusable-action:latest --output-format=png
                outputs:
                    moderately_sensitive:
                        my_figure: output/my_figure.png
        """
        action_file = """
        run: python:latest python reusable_action/main.py
        """
        kwargs["read_file_from_repo"].return_value = action_file
        project = parse_and_validate_project_file(project_file)
        obs_run = project["actions"]["my_action"]["run"]
        exp_run = "python:latest python reusable_action/main.py --output-format=png"
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


def test_get_action_specification_for_cohortextractor_v2_action():
    project_dict = {
        "expectations": {"population_size": 1_000},
        "actions": {
            "generate_cohort_v2": {
                "run": "cohortextractor-v2:latest --output=output/cohort.csv --dummy-data-file dummy.csv",
                "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
            }
        },
    }
    action_id = "generate_cohort_v2"
    action_spec = project.get_action_specification(project_dict, action_id)
    assert (
        action_spec.run
        == """cohortextractor-v2:latest --output=output/cohort.csv --dummy-data-file dummy.csv"""
    )


@pytest.mark.parametrize(
    "args,error",
    [
        (
            "--output=output/cohort1.csv --dummy-data-file dummy.csv",
            "--output in run command and outputs must match",
        ),
        (
            "--output=output/cohort1.csv",
            "--dummy-data-file is required for a local run",
        ),
    ],
)
def test_get_action_specification_for_cohortextractor_v2_errors(args, error):
    project_dict = {
        "expectations": {"population_size": 1_000},
        "actions": {
            "generate_cohort_v2": {
                "run": f"cohortextractor-v2:latest {args}",
                "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
            }
        },
    }
    action_id = "generate_cohort_v2"
    with pytest.raises(ProjectValidationError, match=error):
        project.get_action_specification(project_dict, action_id)
