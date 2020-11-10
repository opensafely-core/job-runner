"""
This sets up some basic logging configuration (log to stderr with a default log
level of INFO) with some job-runner specific tweaks. In particular it supports
automatically including the currently executing Job or JobRequest in the log
output. It also includes the stderr output from any failed attempts to shell
out to external processes.
"""
import contextlib
import logging
import os
import subprocess
import sys
import threading


def configure_logging():
    handler = logging.StreamHandler()
    handler.addFilter(set_log_context)
    handler.setFormatter(JobRunnerFormatter("{context}{message}", style="{"))
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"), handlers=[handler])
    sys.excepthook = show_subprocess_stderr


class JobRunnerFormatter(logging.Formatter):
    def format(self, record):
        """
        Set the `context` record attribute to a string giving the current job
        or job request we're processing
        """
        if hasattr(record, "job"):
            context = f"job#{record.job.id}: "
        elif hasattr(record, "job_request"):
            context = f"job_request#{record.job_request.id}: "
        else:
            context = ""
        record.context = context
        return super().format(record)

    def formatException(self, exc_info):
        """
        We frequently shell out to docker and git and it's very useful to have
        the stderr output in the logs rather than just "this process failed"
        """
        message = super().formatException(exc_info)
        value = exc_info[1]
        if isinstance(value, subprocess.CalledProcessError):
            stderr = value.stderr
            if stderr:
                if isinstance(stderr, bytes):
                    stderr = stderr.decode("utf-8", "ignore")
                message = f"{message}\n\nstderr:\n\n{stderr}"
        return message


def show_subprocess_stderr(typ, value, traceback):
    """
    This applies the same CalledProcessError formatting as in `formatException`
    above but to uncaught exceptions
    """
    sys.__excepthook__(typ, value, traceback)
    if isinstance(value, subprocess.CalledProcessError):
        stderr = value.stderr
        if stderr:
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", "ignore")
            print("\nstderr:\n", file=sys.stderr)
            print(stderr, file=sys.stderr)


class SetLogContext(threading.local):
    """
    A context manager which allows setting `extra` values on all logging calls
    which occur anywhere in its context e.g.

        set_log_context = SetLogContext()
        with set_log_context(foo="bar"):
            log.info("hello world")

    Will result in the equivalent log message to:

        log.info("hello word", extra={"foo": "bar"})

    Uses threading.local for thread safety.
    """

    def __init__(self):
        self.current_context = {}
        self.context_stack = []

    @contextlib.contextmanager
    def __call__(self, **kwargs):
        """
        Create a new logging context with the supplied keyword arguments (in
        addition to any inherited from the current context)
        """
        self.context_stack.append(self.current_context)
        self.current_context = dict(self.current_context, **kwargs)
        try:
            yield
        finally:
            self.current_context = self.context_stack.pop()

    def filter(self, record):
        """
        Apply the current context to a LogRecord
        """
        for key, value in self.current_context.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


set_log_context = SetLogContext()
