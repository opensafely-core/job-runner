import pytest

from runner.utils import safe_join


def test_safe_path():
    safe_join("/workdir", "file.txt") == "/workdir/file.txt"


def test_unsafe_paths_raise():
    with pytest.raises(AssertionError):
        safe_join("/workdir", "../file.txt")
    with pytest.raises(AssertionError):
        safe_join("/workdir", "/file.txt")
