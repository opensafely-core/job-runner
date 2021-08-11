"""
This sets up some basic logging configuration (log to stderr with a default log
level of INFO) with some job-runner specific tweaks. In particular it supports
automatically including the currently executing Job or JobRequest in the log
output. It also includes the stderr output from any failed attempts to shell
out to external processes.
"""
import contextlib
import logging
import logging.handlers
import os
import subprocess
import sys
import threading
import time

DEFAULT_FORMAT = "{asctime} {message} {tags}"


def configure_logging(fmt=DEFAULT_FORMAT, stream=None, status_codes_to_ignore=None):
    formatter = JobRunnerFormatter(fmt, style="{")
    handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(formatter)
    if status_codes_to_ignore:
        handler.addFilter(IgnoreStatusCodes(status_codes_to_ignore))
    handler.addFilter(formatting_filter)

    log_level = os.environ.get("LOGLEVEL", "INFO")
    handlers = [handler]

    # Support a separate log file at level DEBUG, while leaving the default
    # logs untouched. DEBUG logging can be extremely noisy and so we want a way
    # to capture these that doesn't pollute the primary logs.
    debug_log_file = os.environ.get("DEBUG_LOG_FILE")
    if debug_log_file:
        debug_handler = logging.handlers.TimedRotatingFileHandler(
            debug_log_file,
            encoding="utf-8",
            delay=True,
            # Rotate daily, keeping 14 days of backups
            when="D",
            interval=1,
            backupCount=14,
            utc=True,
        )
        debug_handler.setFormatter(formatter)
        debug_handler.addFilter(formatting_filter)
        debug_handler.setLevel("DEBUG")
        handlers.append(debug_handler)
        # Set the default handler to the originally specified log level and
        # then increase the base log level to DEBUG
        handler.setLevel(log_level)
        log_level = "DEBUG"

    logging.basicConfig(level=log_level, handlers=handlers)

    if debug_log_file:
        logging.getLogger(__name__).info(f"Writing DEBUG logs to '{debug_log_file}'")

    # We attach a custom handler for uncaught exceptions to display error
    # output from failed subprocesses
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


def formatting_filter(record):
    """Ensure various record attribute are always available for formatting."""

    ctx = set_log_context.current_context
    job = getattr(record, "job", None) or ctx.get("job")
    req = getattr(record, "job_request", None) or ctx.get("job_request")

    status_code = getattr(record, "status_code", None)
    if job and not status_code:
        status_code = job.status_code

    tags = {}

    if status_code:
        tags["status"] = status_code
    if job:
        tags["project"] = job.project
        tags["action"] = job.action
        tags["id"] = job.id
    if req:
        tags["req"] = req.id

    record.tags = " ".join(f"{k}={v}" for k, v in tags.items())

    # The `action` attribute is only used by format string in "local_run" mode
    # but we make sure it's always available
    record.action = f"{job.action}: " if job else ""

    return True


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


def show_subprocess_stderr(typ, value, traceback):
    """
    This applies the same CalledProcessError formatting as in `JobRunnerFormatter`
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


set_log_context = SetLogContext()
