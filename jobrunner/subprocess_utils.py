"""
Thin wrapper around `subprocess.run` which ensures that any arguments which are
pathlib instances get coerced to strings, which is necessary for them to work
on Windows (but not POSIX). Most of these issues are fixed in Python 3.8 so
it's possible we can drop this later. (The exception being the `env` argument
which the documentation doesn't mention so we'll have to wait and see.)
"""
from pathlib import PurePath
import subprocess


def subprocess_run(cmd_args, **kwargs):
    assert not kwargs.get("shell"), "Don't use shell as we need to work cross-platform"
    cmd_args = list(map(to_str, cmd_args))
    if "cwd" in kwargs:
        kwargs["cwd"] = to_str(kwargs["cwd"])
    if "env" in kwargs:
        kwargs["env"] = {key: to_str(value) for (key, value) in kwargs["env"].items()}
    return subprocess.run(cmd_args, **kwargs)


def to_str(value):
    # PurePath is the base class for all pathlib classes
    if isinstance(value, PurePath):
        return str(value)
    return value
