import os
from pathlib import Path

import pytest

from jobrunner.lib.git import (
    GitRepoNotReachableError,
    GitUnknownRefError,
    checkout_commit,
    commit_already_fetched,
    commit_reachable_from_ref,
    ensure_git_init,
    fetch_commit,
    get_sha_from_remote_ref,
    read_file_from_repo,
)


REPO_FIXTURE = str(Path(__file__).parents[1].resolve() / "fixtures/git-repo")


@pytest.mark.slow_test
def test_read_file_from_repo(tmp_work_dir):
    output = read_file_from_repo(
        "https://github.com/opensafely-core/test-public-repository.git",
        "c1ef0e676ec448b0a49e0073db364f36f6d6d078",
        "README.md",
    )
    assert output == b"# test-public-repository"


@pytest.mark.slow_test
def test_checkout_commit(tmp_work_dir, tmp_path):
    target_dir = tmp_path / "files"
    checkout_commit(
        "https://github.com/opensafely-core/test-public-repository.git",
        "c1ef0e676ec448b0a49e0073db364f36f6d6d078",
        target_dir,
    )
    assert [f.name for f in target_dir.iterdir()] == ["README.md"]


@pytest.mark.slow_test
def test_get_sha_from_remote_ref(tmp_work_dir):
    sha = get_sha_from_remote_ref(
        "https://github.com/opensafely-core/test-public-repository.git",
        "test-tag-dont-delete",
    )
    assert sha == "029a6ff81cb0ab878de24c12bc690969163c5c9e"


@pytest.mark.slow_test
def test_get_sha_from_remote_ref_annotated_tag():
    sha = get_sha_from_remote_ref(
        "https://github.com/opensafely-core/test-public-repository.git",
        "test-annotated-tag-dont-delete",
    )
    assert sha == "3c15ff525001e039d4e27cfc62f652ecad09fde4"


@pytest.mark.slow_test
def test_get_sha_from_remote_ref_missing_ref(tmp_work_dir):
    with pytest.raises(GitUnknownRefError):
        get_sha_from_remote_ref(
            "https://github.com/opensafely-core/test-public-repository.git",
            "no-such-ref",
        )


@pytest.mark.slow_test
def test_get_sha_from_remote_ref_missing_repo(tmp_work_dir):
    with pytest.raises(GitRepoNotReachableError):
        get_sha_from_remote_ref(
            "https://github.com/opensafely-core/no-such-repo.git", "main"
        )


@pytest.mark.slow_test
def test_commit_reachable_from_ref(tmp_work_dir):
    is_reachable_good = commit_reachable_from_ref(
        "https://github.com/opensafely-core/test-public-repository.git",
        "029a6ff81cb0ab878de24c12bc690969163c5c9e",
        "test-branch-dont-delete",
    )
    assert is_reachable_good
    is_reachable_bad = commit_reachable_from_ref(
        "https://github.com/opensafely-core/test-public-repository.git",
        "029a6ff81cb0ab878de24c12bc690969163c5c9e",
        "main",
    )
    assert not is_reachable_bad


# These tests makes request to an actual private GitHub repo and so will only
# work if there's an appropriate access token in the environment


@pytest.mark.skipif(
    not os.environ.get("PRIVATE_REPO_ACCESS_TOKEN"),
    reason="No access token in environment",
)
@pytest.mark.slow_test
def test_read_file_from_private_repo(tmp_work_dir):
    output = read_file_from_repo(
        "https://github.com/opensafely/test-repository.git",
        "d7fe87ab5d6dc97222c4a9dbf7c0fe40fc108c8f",
        "README.md",
    )
    assert output == b"# test-repository\nTesting GH permssions model\n"


@pytest.mark.skipif(
    not os.environ.get("PRIVATE_REPO_ACCESS_TOKEN"),
    reason="No access token in environment",
)
@pytest.mark.slow_test
def test_get_sha_from_remote_ref_private(tmp_work_dir):
    sha = get_sha_from_remote_ref(
        "https://github.com/opensafely/test-repository", "v1.0"
    )
    assert sha == "981ac62ec5620df90556bc18784f06b6e7db7e4d"


# The below tests use a local git repo fixture rather than accessing GitHub
# over HTTPS. This makes them faster, though obviously less complete.


def test_read_file_from_repo_local(tmp_work_dir):
    output = read_file_from_repo(
        REPO_FIXTURE,
        "d1e88b31cbe8f67c58f938adb5ee500d54a69764",
        "project.yaml",
    )
    assert output.startswith(b"version: '1.0'")


def test_checkout_commit_local(tmp_work_dir, tmp_path):
    target_dir = tmp_path / "files"
    checkout_commit(
        REPO_FIXTURE,
        "d1e88b31cbe8f67c58f938adb5ee500d54a69764",
        target_dir,
    )
    assert [f.name for f in target_dir.iterdir()] == ["project.yaml"]


def test_get_sha_from_remote_ref_local(tmp_work_dir):
    sha = get_sha_from_remote_ref(REPO_FIXTURE, "v1")
    assert sha == "d1e88b31cbe8f67c58f938adb5ee500d54a69764"


def test_get_sha_from_remote_ref_local_missing_ref(tmp_work_dir):
    with pytest.raises(GitUnknownRefError):
        get_sha_from_remote_ref(REPO_FIXTURE, "no-such-ref")


def test_get_sha_from_remote_ref_local_missing_repo(tmp_work_dir):
    MISSING_REPO = REPO_FIXTURE + "-no-such-repo"
    with pytest.raises(GitRepoNotReachableError):
        get_sha_from_remote_ref(MISSING_REPO, "v1")


def test_commit_already_fetched(tmp_path):
    commit_sha = "d1e88b31cbe8f67c58f938adb5ee500d54a69764"
    repo_dir = tmp_path / "repo"
    ensure_git_init(repo_dir)
    assert not commit_already_fetched(repo_dir, commit_sha)
    fetch_commit(repo_dir, REPO_FIXTURE, commit_sha)
    assert commit_already_fetched(repo_dir, commit_sha)
