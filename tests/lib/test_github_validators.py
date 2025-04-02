import pytest

from jobrunner.lib.github_validators import (
    GithubValidationError,
    validate_branch_and_commit,
    validate_repo_url,
)


def test_validate_repo_url():
    validate_repo_url(
        "https://github.com/opensafely-core/test-public-repository.git",
        ["opensafely-core"],
    )


def test_validate_repo_url_reject():
    with pytest.raises(
        GithubValidationError,
        match="must belong to one of the following Github organisations",
    ):
        validate_repo_url(
            "https://github.com/not-os/test-private-repository.git",
            ["opensafely-core"],
        )


def test_validate_repo_url_reject_root_url():
    with pytest.raises(
        GithubValidationError,
        match="must start https://github.com",
    ):
        validate_repo_url(
            "https://not-github.com/opensafely-core/test-private-repository.git",
            ["opensafely-core"],
        )


def test_validate_branch_and_commit(tmp_work_dir):
    validate_branch_and_commit(
        "https://github.com/opensafely-core/test-public-repository.git",
        "983d348e3f6bfeeac0cd473d5ab950ce03b022e5",
        "test-branch-dont-delete",
    )


def test_validate_branch_and_commit_no_branch(tmp_work_dir):
    with pytest.raises(GithubValidationError, match="branch name must be supplied"):
        validate_branch_and_commit(
            "https://github.com/opensafely-core/test-public-repository.git",
            "983d348e3f6bfeeac0cd473d5ab950ce03b022e5",
            "",
        )


def test_validate_branch_and_commit_rejects_pull_request_ref(tmp_work_dir):
    with pytest.raises(GithubValidationError, match="Could not find branch"):
        validate_branch_and_commit(
            "https://github.com/opensafely-core/test-public-repository.git",
            "2e59c0ec8d147cb8a475596ba16d6f74ec7ee913",
            "refs/pull/1/head",
        )


def test_validate_branch_and_commit_rejects_unreachable_commit(tmp_work_dir):
    with pytest.raises(GithubValidationError, match="Could not find commit"):
        validate_branch_and_commit(
            "https://github.com/opensafely-core/test-public-repository.git",
            "2e59c0ec8d147cb8a475596ba16d6f74ec7ee913",
            "main",
        )
