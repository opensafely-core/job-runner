from copy import deepcopy
from unittest import mock

import pytest

from jobrunner import reusable_actions
from jobrunner.lib import git, github_validators
from jobrunner.lib.yaml_utils import YAMLError
from jobrunner.models import Job
from jobrunner.reusable_actions import ReusableAction
from tests.factories import JOB_DEFAULTS


@mock.patch.multiple(
    "jobrunner.lib.git",
    get_sha_from_remote_ref=mock.DEFAULT,
    read_file_from_repo=mock.DEFAULT,
)
class TestHandleReusableAction:
    def test_when_not_a_reusable_action(self, **kwargs):
        # Happy path 1
        action_in = "python:latest python analysis/my_action.py"
        action_out = reusable_actions.handle_reusable_action(action_in)[0]
        assert action_in is action_out
        kwargs["get_sha_from_remote_ref"].assert_not_called()
        kwargs["read_file_from_repo"].assert_not_called()

    @mock.patch(
        "jobrunner.reusable_actions.parse_yaml",
        return_value={"run": "python:latest python reusable_action/main.py"},
    )
    def test_when_a_reusable_action_with_options(self, *args, **kwargs):
        # Happy path 2
        action_in = "reusable-action:latest --output-format=png"
        action_out = reusable_actions.handle_reusable_action(action_in)[0]
        assert action_in is not action_out
        assert (
            action_out
            == "python:latest python reusable_action/main.py --output-format=png"
        )

    @mock.patch(
        "jobrunner.reusable_actions.parse_yaml",
        return_value={"run": "python:latest python reusable_action/main.py"},
    )
    def test_when_a_reusable_action_with_arguments(self, *args, **kwargs):
        # Happy path 3
        action_in = "reusable-action:latest output/input.csv"
        action_out = reusable_actions.handle_reusable_action(action_in)[0]
        assert action_in is not action_out
        assert (
            action_out
            == "python:latest python reusable_action/main.py output/input.csv"
        )

    @mock.patch(
        "jobrunner.reusable_actions.parse_yaml",
        return_value={"run": "python:latest python reusable_action/main.py"},
    )
    def test_when_a_reusable_action_with_options_and_arguments(self, *args, **kwargs):
        # Happy path 4
        # We'll use Click's terminology.
        # * Options are optional
        # * Arguments are optional within reason, but are more restricted than options
        # For more information, see:
        # https://click.palletsprojects.com/en/8.0.x/parameters/
        action_in = "reusable-action:latest --output-format=png output/input.csv"
        action_out = reusable_actions.handle_reusable_action(action_in)[0]
        assert action_in is not action_out
        assert (
            action_out
            == "python:latest python reusable_action/main.py --output-format=png output/input.csv"
        )

    def test_with_bad_run_command(self, **kwargs):
        # We don't need to check the scheme, netloc, or org because we add those.
        with pytest.raises(reusable_actions.ReusableActionError):
            reusable_actions.handle_reusable_action(
                "../my-bad-org/reusable-action:latest"
            )

    @pytest.mark.parametrize(
        "side_effect", [git.GitUnknownRefError, git.GitRepoNotReachableError]
    )
    def test_with_bad_remote_ref(self, side_effect, **kwargs):
        kwargs["get_sha_from_remote_ref"].side_effect = side_effect
        with pytest.raises(reusable_actions.ReusableActionError):
            reusable_actions.handle_reusable_action("reusable-action:latest")

    @pytest.mark.parametrize(
        "side_effect", [github_validators.GithubValidationError, git.GitError]
    )
    @mock.patch("jobrunner.reusable_actions.validate_branch_and_commit")
    def test_with_bad_commit(self, patched, side_effect, **kwargs):
        patched.side_effect = side_effect
        with pytest.raises(reusable_actions.ReusableActionError):
            reusable_actions.handle_reusable_action("reusable-action:latest")

    @pytest.mark.parametrize("side_effect", [git.GitError, git.GitFileNotFoundError])
    def test_with_bad_file(self, side_effect, **kwargs):
        kwargs["read_file_from_repo"].side_effect = side_effect
        with pytest.raises(reusable_actions.ReusableActionError):
            reusable_actions.handle_reusable_action("reusable-action:latest")

    @mock.patch(
        "jobrunner.reusable_actions.parse_yaml",
        side_effect=YAMLError,
    )
    def test_with_bad_yaml(self, *args, **kwargs):
        with pytest.raises(reusable_actions.ReusableActionError):
            reusable_actions.handle_reusable_action("reusable-action:latest")

    @mock.patch("jobrunner.reusable_actions.parse_yaml", return_value={})
    def test_with_bad_action_config(self, *args, **kwargs):
        with pytest.raises(reusable_actions.ReusableActionError):
            reusable_actions.handle_reusable_action("reusable-action:latest")

    @pytest.mark.parametrize(
        "action",
        [
            "notanaction:v1",
            # These are valid runtimes, but not allowed in re-usable actions
            "ehrql:v1 generate-dataset dataset.py --output dataset.csv",
        ],
    )
    def test_reusable_action_with_invalid_runtime(self, action, *args, **kwargs):
        reusable_action = ReusableAction(
            repo_url="foo", commit="bar", action_file=f"run: {action}".encode("ascii")
        )
        with pytest.raises(reusable_actions.ReusableActionError):
            reusable_actions.apply_reusable_action(["foo:v1"], reusable_action)

    @mock.patch(
        "jobrunner.reusable_actions.parse_yaml",
        return_value={"run": "python:latest python reusable_action/main.py"},
    )
    def test_resolve_reusable_action_references(self, *args, **kwargs):
        job1_data = deepcopy(JOB_DEFAULTS)
        job1_data["run_command"] = "python:v1 myscript.py"

        job2_data = deepcopy(JOB_DEFAULTS)
        job2_data["run_command"] = "reusable-action:latest --output-format=png"

        jobs = [Job(**job1_data), Job(**job2_data)]

        reusable_actions.resolve_reusable_action_references(jobs)
        assert jobs[0].run_command == "python:v1 myscript.py"
        assert (
            jobs[1].run_command
            == "python:latest python reusable_action/main.py --output-format=png"
        )

    @mock.patch(
        "jobrunner.reusable_actions.parse_yaml",
        return_value={"run": "python:latest python reusable_action/main.py"},
    )
    @mock.patch(
        "jobrunner.reusable_actions.validate_branch_and_commit",
        side_effect=[mock.DEFAULT, git.GitUnknownRefError],
    )
    def test_resolve_reusable_action_references_error(self, *args, **kwargs):
        job1_data = deepcopy(JOB_DEFAULTS)
        job1_data["run_command"] = "reusable-action:latest --output-format=jpg"

        job2_data = deepcopy(JOB_DEFAULTS)
        job2_data["run_command"] = "reusable-action:latest --output-format=png"

        jobs = [Job(**job1_data), Job(**job2_data)]

        with pytest.raises(reusable_actions.ReusableActionError):
            reusable_actions.resolve_reusable_action_references(jobs)

        # job1 is OK, resolved
        assert (
            jobs[0].run_command
            == "python:latest python reusable_action/main.py --output-format=jpg"
        )
        # job2 raised exception, not resolved
        assert jobs[1].run_command == "reusable-action:latest --output-format=png"
