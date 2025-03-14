import time

import pytest

from jobrunner.executors.logging import LoggingExecutor
from jobrunner.job_executor import ExecutorState, JobDefinition, JobStatus, Study
from tests.fakes import RecordingExecutor


methods = ["get_status", "prepare", "execute", "finalize", "terminate", "cleanup"]


@pytest.mark.parametrize("method", methods)
def test_logs(method):
    executor = LoggingExecutor(
        RecordingExecutor(JobStatus(ExecutorState.EXECUTING, "doing the thing"))
    )

    log = None

    def record(msg):
        nonlocal log
        log = msg

    executor._logger.info = record

    getattr(executor, method)(create_job_definition("the-job-id"))

    assert "the-job-id" in log
    assert "doing the thing" in log
    assert ExecutorState.EXECUTING.name in log


@pytest.mark.parametrize("method", methods)
def test_delegates(method):
    status = JobStatus(ExecutorState.EXECUTED)
    recording = RecordingExecutor(status)
    executor = LoggingExecutor(recording)
    job_definition = create_job_definition("the-job-id")

    returned_status = getattr(executor, method)(job_definition)

    assert recording.job_definition == job_definition
    assert returned_status == status


def create_job_definition(job_id="a-job-id"):
    job_definition = JobDefinition(
        id=job_id,
        job_request_id="test_request_id",
        study=Study(git_repo_url="", commit=""),
        workspace="",
        action="",
        created_at=int(time.time()),
        image="",
        args=[],
        env={},
        inputs=[],
        output_spec={},
        allow_database_access=False,
        level4_max_csv_rows=None,
        level4_max_filesize=None,
    )
    return job_definition
