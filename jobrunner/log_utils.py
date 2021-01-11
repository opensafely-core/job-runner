"""
This sets up some basic logging configuration (log to stderr with a default log
level of INFO) with some job-runner specific tweaks. In particular it supports
automatically including the currently executing Job or JobRequest in the log
output. It also includes the stderr output from any failed attempts to shell
out to external processes.
"""
import contextlib
import datetime
import logging
import os
import subprocess
import sys
import threading
import time


DEFAULT_FORMAT = "{asctime} {message} {tags}"


def formatting_filter(record):
    """Ensure various record attribute are always available for formatting."""

    # ensure this are always available for static formatting
    record.action = ""

    tags = {}
    ctx = set_log_context.current_context
    job = getattr(record, "job", None) or ctx.get("job")
    req = getattr(record, "job_request", None) or ctx.get("job_request")

    status_code = getattr(record, "status_code", None)
    if status_code:
        tags["status"] = record.status_code

    if job:
        # preserve short action for local run formatting
        record.action = job.action + ": "
        if "status" not in tags and job.status_code:
            tags["status"] = job.status_code
        tags["project"] = job.project
        tags["action"] = job.action
        tags["id"] = job.id

    if req:
        tags["req"] = req.id

    record.tags = " ".join(f"{k}={v}" for k, v in tags.items())

    return True


def configure_logging(fmt=DEFAULT_FORMAT, stream=None, status_codes_to_ignore=None):
    formatter = JobRunnerFormatter(fmt, style="{")
    handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(formatter)
    if status_codes_to_ignore:
        handler.addFilter(IgnoreStatusCodes(status_codes_to_ignore))
    handler.addFilter(formatting_filter)
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"), handlers=[handler])
    sys.excepthook = show_subprocess_stderr


class JobRunnerFormatter(logging.Formatter):

    converter = time.gmtime  # utc rather than local
    default_msec_format = "%s.%03dZ"  # s/,/. and append Z

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


class IgnoreStatusCodes:
    """
    Skip log lines which have certain status codes
    """

    def __init__(self, status_codes_to_ignore):
        self.status_codes_to_ignore = set(status_codes_to_ignore)

    def filter(self, record):
        status_code = getattr(record, "status_code", None)
        return status_code not in self.status_codes_to_ignore


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
        if hasattr(record, "status_code"):
            return record.status_code not in self.status_codes_to_ignore
        return True


set_log_context = SetLogContext()
