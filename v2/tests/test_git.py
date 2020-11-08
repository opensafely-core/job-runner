import os
from pathlib import Path

import pytest

from jobrunner.git import read_file_from_repo, checkout_commit, get_sha_from_remote_ref


REPO_FIXTURE = str(Path(__file__).parent.resolve() / "fixtures/git-repo")


remote_repo_test = pytest.mark.skipif(
    bool(os.environ.get("LOCAL_TESTS_ONLY")),
    reason="Skipping tests which talk to GitHub",
)


@remote_repo_test
def test_read_file_from_repo(tmp_work_dir):
    output = read_file_from_repo(
        "https://github.com/opensafely/documentation.git",
        "e28665e3796841c6b42f995f9de28f7177ec5e91",
        "README.md",
    )
    assert output == b"# documentation"


@remote_repo_test
def test_checkout_commit(tmp_work_dir, tmp_path):
    target_dir = tmp_path / "files"
    checkout_commit(
        "https://github.com/opensafely/documentation.git",
        "e28665e3796841c6b42f995f9de28f7177ec5e91",
        target_dir,
    )
    assert [f.name for f in target_dir.iterdir()] == ["README.md"]


@remote_repo_test
def test_get_sha_from_remote_ref(tmp_work_dir):
    sha = get_sha_from_remote_ref(
        "https://github.com/opensafely/cohort-extractor", "v1.0.0"
    )
    assert sha == "d78522cce38e6f431353e9e96de62d49b7ee86ea"


@pytest.mark.skipif(
    not os.environ.get("PRIVATE_REPO_ACCESS_TOKEN"),
    reason="No access token in environment",
)
@remote_repo_test
def test_read_file_from_private_repo(tmp_work_dir):
    output = read_file_from_repo(
        "https://github.com/opensafely/test-repository.git",
        "d7fe87ab5d6dc97222c4a9dbf7c0fe40fc108c8f",
        "README.md",
    )
    assert output == b"# test-repository\nTesting GH permssions model\n"


# The below tests use a local git repo fixture rather than accessing GitHub
# over HTTPS. This makes them faster, though obviously less complete.


def test_read_file_from_repo_local(tmp_work_dir):
    output = read_file_from_repo(
        REPO_FIXTURE, "d1e88b31cbe8f67c58f938adb5ee500d54a69764", "project.yaml",
    )
    assert output.startswith(b"version: '1.0'")


def test_checkout_commit_local(tmp_work_dir, tmp_path):
    target_dir = tmp_path / "files"
    checkout_commit(
        REPO_FIXTURE, "d1e88b31cbe8f67c58f938adb5ee500d54a69764", target_dir,
    )
    assert [f.name for f in target_dir.iterdir()] == ["project.yaml"]


def test_get_sha_from_remote_ref_local(tmp_work_dir):
    sha = get_sha_from_remote_ref(REPO_FIXTURE, "v1")
    assert sha == "d1e88b31cbe8f67c58f938adb5ee500d54a69764"
