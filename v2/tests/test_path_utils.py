import pytest


from jobrunner.path_utils import assert_is_safe_path, UnsafePathError


def test_assert_is_safe_path():
    assert_is_safe_path("foo/bar/*.txt")
    assert_is_safe_path("foo")
    bad_paths = [
        "/abs/path",
        "ends/in/slash/",
        "not//canonical",
        "path/../traversal",
        "c:/windows/absolute",
    ]
    for path in bad_paths:
        with pytest.raises(UnsafePathError):
            assert_is_safe_path(path)
