import logging
import time
from datetime import datetime

from jobrunner.cli import local_run
from jobrunner.lib import log_utils
from jobrunner.models import Job, JobRequest


FROZEN_TIMESTAMP = 1608568119.1467905
FROZEN_TIMESTRING = datetime.utcfromtimestamp(FROZEN_TIMESTAMP).isoformat()

repo_url = "https://github.com/opensafely/project"
test_job = Job(id="id", action="action", repo_url=repo_url)
test_request = JobRequest(
    id="request",
    repo_url=repo_url,
    workspace="workspace",
    commit="commit",
    requested_actions=["action"],
    cancelled_actions=[],
    database_name="dummy",
)


def test_formatting_filter():
    record = logging.makeLogRecord({})
    assert log_utils.formatting_filter(record)
    assert record.action == ""

    record = logging.makeLogRecord({"job": test_job})
    assert log_utils.formatting_filter(record)
    assert record.action == "action: "
    assert record.tags == "project=project action=action id=id"

    record = logging.makeLogRecord({"job": test_job, "status_code": "code"})
    assert log_utils.formatting_filter(record)
    assert record.tags == "status=code project=project action=action id=id"

    test_job2 = Job(id="id", action="action", repo_url=repo_url, status_code="code")
    record = logging.makeLogRecord({"job": test_job2})
    assert log_utils.formatting_filter(record)
    assert record.tags == "status=code project=project action=action id=id"

    record = logging.makeLogRecord({"job": test_job, "job_request": test_request})
    assert log_utils.formatting_filter(record)
    assert record.tags == "project=project action=action id=id req=request"

    record = logging.makeLogRecord({"status_code": ""})
    assert log_utils.formatting_filter(record)
    assert record.tags == ""


def test_formatting_filter_with_context():
    record = logging.makeLogRecord({})
    with log_utils.set_log_context(job=test_job):
        assert log_utils.formatting_filter(record)
    assert record.action == "action: "
    assert record.tags == "project=project action=action id=id"

    record = logging.makeLogRecord({"status_code": "code"})
    with log_utils.set_log_context(job=test_job):
        assert log_utils.formatting_filter(record)
    assert record.tags == "status=code project=project action=action id=id"

    record = logging.makeLogRecord({})
    with log_utils.set_log_context(job=test_job, job_request=test_request):
        assert log_utils.formatting_filter(record)
    assert record.tags == "project=project action=action id=id req=request"


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
        "status=status project=project action=action id=id req=request"
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
