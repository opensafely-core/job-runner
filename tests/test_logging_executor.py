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

    getattr(executor, method)(job_definition("the-job-id"))

    assert "the-job-id" in log
    assert "doing the thing" in log
    assert ExecutorState.EXECUTING.name in log


@pytest.mark.parametrize("method", methods)
def test_delegates(method):
    status = JobStatus(ExecutorState.EXECUTED)
    recording = RecordingExecutor(status)
    executor = LoggingExecutor(recording)
    job = job_definition("the-job-id")

    returned_status = getattr(executor, method)(job)

    assert recording.job == job
    assert returned_status == status


def job_definition(job_id="a-job-id"):
    job = JobDefinition(
        id=job_id,
        study=Study(git_repo_url="", commit=""),
        workspace="",
        action="",
        image="",
        args=[],
        env={},
        inputs=[],
        output_spec={},
        allow_database_access=False,
    )
    return job
