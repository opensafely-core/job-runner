import os

import pytest

from jobrunner.git import read_file_from_repo, checkout_commit, get_sha_from_remote_ref


@pytest.fixture()
def setup_config(tmp_path, monkeypatch):
    monkeypatch.setattr("jobrunner.config.GIT_REPO_DIR", tmp_path)


def test_read_file_from_repo(setup_config):
    output = read_file_from_repo(
        "https://github.com/opensafely/documentation.git",
        "e28665e3796841c6b42f995f9de28f7177ec5e91",
        "README.md",
    )
    assert output == b"# documentation"


def test_checkout_commit(setup_config, tmp_path):
    target_dir = tmp_path / "files"
    checkout_commit(
        "https://github.com/opensafely/documentation.git",
        "e28665e3796841c6b42f995f9de28f7177ec5e91",
        target_dir,
    )
    assert [f.name for f in target_dir.iterdir()] == ["README.md"]


def test_get_sha_from_remote_ref():
    sha = get_sha_from_remote_ref(
        "https://github.com/opensafely/cohort-extractor", "v1.0.0"
    )
    assert sha == b"d78522cce38e6f431353e9e96de62d49b7ee86ea"


@pytest.mark.skipif(
    not os.environ.get("PRIVATE_REPO_ACCESS_TOKEN"),
    reason="No access token in environment",
)
def test_read_file_from_private_repo(setup_config):
    output = read_file_from_repo(
        "https://github.com/opensafely/test-repository.git",
        "d7fe87ab5d6dc97222c4a9dbf7c0fe40fc108c8f",
        "README.md",
    )
    assert output == b"# test-repository\nTesting GH permssions model\n"
