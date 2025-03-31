import logging
import subprocess
import sys
import time
from datetime import datetime

import pytest

from jobrunner.cli import local_run
from jobrunner.lib import log_utils
from jobrunner.models import Job, JobRequest


FROZEN_TIMESTAMP = 1608568119.1467905
FROZEN_TIMESTRING = datetime.utcfromtimestamp(FROZEN_TIMESTAMP).isoformat()

repo_url = "https://github.com/opensafely/project"
test_job = Job(
    id="id",
    action="action",
    repo_url=repo_url,
    workspace="workspace",
)
test_request = JobRequest(
    id="request",
    repo_url=repo_url,
    workspace="workspace",
    commit="commit",
    requested_actions=["action"],
    cancelled_actions=[],
    codelists_ok=True,
    database_name="dummy",
)


def test_formatting_filter():
    record = logging.makeLogRecord({})
    assert log_utils.formatting_filter(record)
    assert record.action == ""

    record = logging.makeLogRecord({"job": test_job})
    assert log_utils.formatting_filter(record)
    assert record.action == "action: "
    assert record.tags == "workspace=workspace action=action id=id"

    record = logging.makeLogRecord({"job": test_job, "status_code": "code"})
    assert log_utils.formatting_filter(record)
    assert record.tags == "status=code workspace=workspace action=action id=id"

    test_job2 = Job(
        id="id",
        action="action",
        repo_url=repo_url,
        status_code="code",
        workspace="workspace",
    )
    record = logging.makeLogRecord({"job": test_job2})
    assert log_utils.formatting_filter(record)
    assert record.tags == "status=code workspace=workspace action=action id=id"

    record = logging.makeLogRecord({"job": test_job, "job_request": test_request})
    assert log_utils.formatting_filter(record)
    assert record.tags == "workspace=workspace action=action id=id req=request"

    record = logging.makeLogRecord({"status_code": ""})
    assert log_utils.formatting_filter(record)
    assert record.tags == ""


def test_formatting_filter_with_context():
    record = logging.makeLogRecord({})
    with log_utils.set_log_context(job=test_job):
        assert log_utils.formatting_filter(record)
    assert record.action == "action: "
    assert record.tags == "workspace=workspace action=action id=id"

    record = logging.makeLogRecord({"status_code": "code"})
    with log_utils.set_log_context(job=test_job):
        assert log_utils.formatting_filter(record)
    assert record.tags == "status=code workspace=workspace action=action id=id"

    record = logging.makeLogRecord({})
    with log_utils.set_log_context(job=test_job, job_request=test_request):
        assert log_utils.formatting_filter(record)
    assert record.tags == "workspace=workspace action=action id=id req=request"


def test_jobrunner_formatter_default(monkeypatch):
    monkeypatch.setattr(time, "time", lambda: FROZEN_TIMESTAMP)
    record = logging.makeLogRecord(
        {
            "msg": "message",
            "job": test_job,
            "job_request": test_request,
            "status_code": "status",
        }
    )
    log_utils.formatting_filter(record)
    formatter = log_utils.JobRunnerFormatter(log_utils.DEFAULT_FORMAT, style="{")
    assert formatter.format(record) == (
        "2020-12-21 16:28:39.146Z message "
        "status=status workspace=workspace action=action id=id req=request"
    )


def test_jobrunner_formatter_local_run(monkeypatch):
    monkeypatch.setattr(time, "time", lambda: FROZEN_TIMESTAMP)
    record = logging.makeLogRecord(
        {
            "msg": "message",
            "job": test_job,
            "job_request": test_request,
            "status_code": "status",
        }
    )
    log_utils.formatting_filter(record)
    formatter = log_utils.JobRunnerFormatter(local_run.LOCAL_RUN_FORMAT, style="{")
    assert formatter.format(record) == "action: message"


def test_jobrunner_formatter_with_exception():
    exc_info = None
    try:
        raise Exception("foo")
    except Exception:
        exc_info = sys.exc_info()

    record = logging.makeLogRecord({"level": logging.ERROR, "exc_info": exc_info})
    log_utils.formatting_filter(record)
    formatter = log_utils.JobRunnerFormatter(log_utils.DEFAULT_FORMAT, style="{")
    formatted_log = formatter.format(record)
    assert "Traceback (most recent call last):" in formatted_log
    assert "Exception: foo" in formatted_log


@pytest.mark.parametrize(
    "stderr,expected_stderr_output",
    [
        ("This is the stderr", "\n\nstderr:\n\nThis is the stderr"),
        (b"This is the stderr", "\n\nstderr:\n\nThis is the stderr"),
        (None, ""),
    ],
)
def test_jobrunner_formatter_with_called_process_exception_includes_stderr(
    stderr, expected_stderr_output
):
    exc_info = None
    try:
        raise subprocess.CalledProcessError(
            returncode=1, cmd="foo", output=None, stderr=stderr
        )
    except subprocess.CalledProcessError:
        exc_info = sys.exc_info()

    record = logging.makeLogRecord({"level": logging.ERROR, "exc_info": exc_info})
    log_utils.formatting_filter(record)
    formatter = log_utils.JobRunnerFormatter(log_utils.DEFAULT_FORMAT, style="{")
    formatted_log = formatter.format(record)
    assert "Traceback (most recent call last):" in formatted_log
    expected_msg = (
        "Command 'foo' returned non-zero exit status 1." + expected_stderr_output
    )
    assert expected_msg in formatted_log
