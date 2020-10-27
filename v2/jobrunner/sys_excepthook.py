import subprocess
import sys


def show_subprocess_stderr(typ, value, traceback):
    """
    If a subprocess.CalledProcessError ends up being uncaught, append its
    stderr value (if any) to the standard Python traceback
    """
    sys.__excepthook__(typ, value, traceback)
    if isinstance(value, subprocess.CalledProcessError):
        stderr = value.stderr
        if stderr:
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", "ignore")
            print("\nstderr output:\n", file=sys.stderr)
            print(stderr, file=sys.stderr)


def add_excepthook():
    sys.excepthook = show_subprocess_stderr
