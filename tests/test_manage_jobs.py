import os
import tempfile
from unittest import mock

import pytest

from jobrunner import config, models
from jobrunner.manage_jobs import create_and_populate_volume, delete_files


def is_filesystem_case_sensitive():
    """Returns True if the filesystem is case sensitive; otherwise returns False."""
    # Return a file-like object with some upper-case letters in its name that is deleted
    # as soon as it is closed.
    with tempfile.NamedTemporaryFile(prefix="TEMPORARY_FILE_") as temporary_file:
        # The name property contains the path to the file.
        return not os.path.exists(temporary_file.name.lower())


def test_delete_files(tmp_path):
    (tmp_path / "foo1").touch()
    (tmp_path / "foo2").touch()
    (tmp_path / "foo3").touch()
    delete_files(tmp_path, ["foo1", "foo2", "foo3"], files_to_keep=["FOO1", "foo2"])
    filenames = [f.name for f in tmp_path.iterdir()]
    expected = ["foo1", "foo2"] if not is_filesystem_case_sensitive() else ["foo2"]
    assert filenames == expected


@mock.patch.multiple(
    "jobrunner.docker",
    create_volume=mock.DEFAULT,
    copy_to_volume=mock.DEFAULT,
)
@mock.patch.multiple(
    "jobrunner.manage_jobs",
    copy_local_workspace_to_volume=mock.DEFAULT,
    copy_git_commit_to_volume=mock.DEFAULT,
)
class TestCreateAndPopulateVolume:
    # We patch docker to speed up the tests; we patch manage_jobs to test that the
    # expected path was followed.

    @pytest.fixture
    def action_job(self):
        """Returns a minimal Job instance that represents an action."""
        return models.Job(
            repo_url="opensafely/my-study",
            requires_outputs_from=[],
            workspace="output",
        )

    @pytest.fixture
    def reusable_action_job(self):
        """Returns a minimal Job instance that represents a reusable action."""
        return models.Job(
            repo_url="opensafely-actions/my-reusable-action",
            commit="the-sha-for-this-commit",
            requires_outputs_from=[],
            workspace="output",
        )

    @mock.patch.object(config, "LOCAL_RUN_MODE", True)
    def test_is_local_run_not_reusable_action(self, *, action_job, **kwargs):
        mocked_copy_local_workspace_to_volume = kwargs["copy_local_workspace_to_volume"]
        mocked_copy_git_commit_to_volume = kwargs["copy_git_commit_to_volume"]

        create_and_populate_volume(action_job)
        mocked_copy_local_workspace_to_volume.assert_called_once()
        mocked_copy_git_commit_to_volume.assert_not_called()

    @mock.patch.object(config, "LOCAL_RUN_MODE", True)
    def test_is_local_run_is_reusable_action(self, *, reusable_action_job, **kwargs):
        mocked_copy_local_workspace_to_volume = kwargs["copy_local_workspace_to_volume"]
        mocked_copy_git_commit_to_volume = kwargs["copy_git_commit_to_volume"]

        create_and_populate_volume(reusable_action_job)
        mocked_copy_local_workspace_to_volume.assert_not_called()
        mocked_copy_git_commit_to_volume.assert_called_once()

    @mock.patch.object(config, "LOCAL_RUN_MODE", False)
    def test_not_local_run_not_reusable_action(self, *, action_job, **kwargs):
        mocked_copy_local_workspace_to_volume = kwargs["copy_local_workspace_to_volume"]
        mocked_copy_git_commit_to_volume = kwargs["copy_git_commit_to_volume"]

        create_and_populate_volume(action_job)
        mocked_copy_local_workspace_to_volume.assert_not_called()
        mocked_copy_git_commit_to_volume.assert_called_once()

    @mock.patch.object(config, "LOCAL_RUN_MODE", False)
    def test_not_local_run_is_reusable_action(self, *, reusable_action_job, **kwargs):
        mocked_copy_local_workspace_to_volume = kwargs["copy_local_workspace_to_volume"]
        mocked_copy_git_commit_to_volume = kwargs["copy_git_commit_to_volume"]

        create_and_populate_volume(reusable_action_job)
        mocked_copy_local_workspace_to_volume.assert_not_called()
        mocked_copy_git_commit_to_volume.assert_called_once()
