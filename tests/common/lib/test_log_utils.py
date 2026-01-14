import logging
import subprocess
import sys
import time
from datetime import datetime

import pytest

from common.job_executor import JobDefinition
from common.lib import log_utils
from common.schema import AgentTask
from controller.models import Job, TaskType


FROZEN_TIMESTAMP = 1608568119.1467905
FROZEN_TIMESTRING = datetime.utcfromtimestamp(FROZEN_TIMESTAMP).isoformat()

repo_url = "https://github.com/opensafely/project"
test_job = Job(
    id="id",
    action="action",
    repo_url=repo_url,
    workspace="workspace",
)
test_job_definition = JobDefinition(
    id="job-def",
    rap_id="request1",
    task_id="job-def-001",
    study=None,
    workspace="workspace",
    action="",
    created_at=None,
    user="testuser",
    image="",
    image_sha=None,
    args=[],
    env={},
    inputs=[],
    input_job_ids=[],
    output_spec={},
    allow_database_access=False,
    level4_file_types=[],
    level4_max_csv_rows=0,
    level4_max_filesize=0,
)
test_task = AgentTask(
    id="id-001", type=TaskType.RUNJOB, backend="", definition={}, attributes={}
)


def test_formatting_filter():
    record = logging.makeLogRecord({})
    assert log_utils.formatting_filter(record)

    record = logging.makeLogRecord({"job": test_job})
    assert log_utils.formatting_filter(record)
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

    record = logging.makeLogRecord({"job": test_job})
    assert log_utils.formatting_filter(record)
    assert record.tags == "workspace=workspace action=action id=id"

    record = logging.makeLogRecord({"status_code": ""})
    assert log_utils.formatting_filter(record)
    assert record.tags == ""

    record = logging.makeLogRecord({"job_definition": test_job_definition})
    assert log_utils.formatting_filter(record)
    assert record.tags == "id=job-def"

    record = logging.makeLogRecord(
        {"task": test_task, "job_definition": test_job_definition}
    )
    assert log_utils.formatting_filter(record)
    assert record.tags == "id=job-def task=id-001 task_type=RUNJOB"


def test_formatting_filter_with_context():
    record = logging.makeLogRecord({})
    with log_utils.set_log_context(job=test_job):
        assert log_utils.formatting_filter(record)
    assert record.tags == "workspace=workspace action=action id=id"

    record = logging.makeLogRecord({"status_code": "code"})
    with log_utils.set_log_context(job=test_job):
        assert log_utils.formatting_filter(record)
    assert record.tags == "status=code workspace=workspace action=action id=id"

    record = logging.makeLogRecord({})
    with log_utils.set_log_context(job=test_job):
        assert log_utils.formatting_filter(record)
    assert record.tags == "workspace=workspace action=action id=id"

    record = logging.makeLogRecord({})
    with log_utils.set_log_context(job_definition=test_job_definition):
        assert log_utils.formatting_filter(record)
    assert record.tags == "id=job-def"

    record = logging.makeLogRecord({})
    with log_utils.set_log_context(job_definition=test_job_definition, task=test_task):
        assert log_utils.formatting_filter(record)
    assert record.tags == "id=job-def task=id-001 task_type=RUNJOB"


def test_jobrunner_formatter_default(monkeypatch):
    monkeypatch.setattr(time, "time", lambda: FROZEN_TIMESTAMP)
    record = logging.makeLogRecord(
        {
            "msg": "message",
            "job": test_job,
            "status_code": "status",
        }
    )
    log_utils.formatting_filter(record)
    formatter = log_utils.JobRunnerFormatter(log_utils.DEFAULT_FORMAT, style="{")
    assert formatter.format(record) == (
        "2020-12-21 16:28:39.146Z message "
        "status=status workspace=workspace action=action id=id"
    )


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
