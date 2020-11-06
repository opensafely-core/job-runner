from pathlib import PureWindowsPath, PurePosixPath
from posixpath import normpath


class UnsafePathError(Exception):
    pass


def assert_is_safe_path(path):
    """
    A safe path, for our purposes:
      * has no backslashes;
      * does not end in a slash.
      * has no path-traversal elements;
      * is relative.

    In fact these paths get converted into regular expressions and matched with
    a `find` command so there shouldn't be any possibility of a path traversal
    attack anyway. But it's still good to ensure that they are well-formed.
    """
    if "\\" in path:
        raise UnsafePathError("contains backslash")
    if path.endswith("/"):
        raise UnsafePathError("ends with forward slash")
    if normpath(path) != path:
        raise UnsafePathError("contains path-traversal elements")
    # Windows has a different notion of aboslute paths (e.g c:/foo) so we check
    # for both platforms
    if PurePosixPath(path).is_absolute() or PureWindowsPath(path).is_absolute():
        raise UnsafePathError("is an absolute path")
