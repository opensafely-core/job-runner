import pytest

from jobrunner.lib.github_validators import (
    GithubValidationError,
    validate_branch_and_commit,
)


def test_validate_branch_and_commit():
    validate_branch_and_commit(
        "https://github.com/opensafely-core/test-public-repository.git",
        "983d348e3f6bfeeac0cd473d5ab950ce03b022e5",
        "test-branch-dont-delete",
    )


def test_validate_branch_and_commit_rejects_pull_request_ref():
    with pytest.raises(GithubValidationError, match="Could not find branch"):
        validate_branch_and_commit(
            "https://github.com/opensafely-core/test-public-repository.git",
            "2e59c0ec8d147cb8a475596ba16d6f74ec7ee913",
            "refs/pull/1/head",
        )


def test_validate_branch_and_commit_rejects_unreachable_commit():
    with pytest.raises(GithubValidationError, match="Could not find commit"):
        validate_branch_and_commit(
            "https://github.com/opensafely-core/test-public-repository.git",
            "2e59c0ec8d147cb8a475596ba16d6f74ec7ee913",
            "main",
        )
