import pytest

from runner.utils import make_volume_name, safe_join


def test_safe_path():
    safe_join("/workdir", "file.txt") == "/workdir/file.txt"


def test_unsafe_paths_raise():
    with pytest.raises(AssertionError):
        safe_join("/workdir", "../file.txt")
    with pytest.raises(AssertionError):
        safe_join("/workdir", "/file.txt")


def test_make_volume_name():
    workspace = {
        "repo": "https://github.com/opensafely/hiv-research/",
        "name": "tofu",
        "branch": "feasibility-no",
        "owner": "me",
        "db": "full",
    }
    assert (
        make_volume_name(workspace)
        == "https-github-com-opensafely-hiv-research-feasibility-no-full-me-tofu"
    )
