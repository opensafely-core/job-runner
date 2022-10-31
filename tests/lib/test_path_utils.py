import pathlib

import pytest

from jobrunner.lib import path_utils


@pytest.mark.parametrize(
    "path,expected",
    [
        (r"foo/bar", "foo/bar"),
        (r"foo\bar", "foo/bar"),
        (pathlib.PurePosixPath("foo/bar"), "foo/bar"),
        (pathlib.PurePosixPath(r"foo\bar"), r"foo\bar"),
        (pathlib.PureWindowsPath("foo/bar"), "foo/bar"),
        (pathlib.PureWindowsPath(r"foo\bar"), "foo/bar"),
    ],
)
def test_ensure_unix_path(path, expected):
    assert path_utils.ensure_unix_path(path) == expected
