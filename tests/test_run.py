import random
import pytest

from jobrunner.models import State, StatusCode
from jobrunner.manage_jobs import JobError
from jobrunner import config
from jobrunner.lib.database import transaction
from jobrunner import run

from tests.factories import TestJobAPI


@pytest.fixture()
def db(monkeypatch):
    """Create a throwaway db."""
    monkeypatch.setattr(config, "DATABASE_FILE", ":memory:{random.randrange(sys.maxsize)}")


def test_handle_pending_job_success(db):
    api = TestJobAPI()
    job = api.add_test_job(state=State.PENDING)

    run.handle_pending_job_api(job, api)

    assert job.id in api.jobs_run

    assert job.status_message == "Running"
    assert job.state == State.RUNNING


def test_handle_pending_job_cancelled(db):
    api = TestJobAPI()
    job = api.add_test_job(state=State.PENDING, cancelled=True)

    run.handle_pending_job_api(job, api)

    assert job.id not in api.jobs_run
    assert job.id not in api.jobs_terminated

    assert job.state == State.FAILED
    assert job.status_message == "Cancelled by user"
    assert job.status_code == StatusCode.CANCELLED_BY_USER


def test_handle_pending_job_dependency_failed(db):
    api = TestJobAPI()
    dependency = api.add_test_job(state=State.FAILED)
    job = api.add_test_job(
        job_request_id=dependency.job_request_id,
        action="action2",
        state=State.PENDING, 
        wait_for_job_ids=[dependency.id])

    run.handle_pending_job_api(job, api)

    assert job.id not in api.jobs_run

    assert job.state == State.FAILED
    assert job.status_message == "Not starting as dependency failed"
    assert job.status_code == StatusCode.DEPENDENCY_FAILED


def test_handle_pending_job_waiting_on_dependency(db):
    api = TestJobAPI()
    dependency = api.add_test_job(state=State.RUNNING)

    job = api.add_test_job(
        job_request_id=dependency.job_request_id,
        action="action2",
        state=State.PENDING, 
        wait_for_job_ids=[dependency.id])

    run.handle_pending_job_api(job, api)

    assert job.id not in api.jobs_run

    assert job.state == State.PENDING
    assert job.status_message == "Waiting on dependencies"
    assert job.status_code == StatusCode.WAITING_ON_DEPENDENCIES


def test_handle_pending_job_waiting_on_workers(db, monkeypatch):
    # hack to ensure no workers available
    monkeypatch.setattr(config, "MAX_WORKERS", 0)
    api = TestJobAPI()
    job = api.add_test_job(state=State.PENDING)

    run.handle_pending_job_api(job, api)

    assert job.id not in api.jobs_run

    assert job.state == State.PENDING
    assert job.status_message == "Waiting on available workers"
    assert job.status_code == StatusCode.WAITING_ON_WORKERS


def test_handle_pending_job_run_job_error(db):
    api = TestJobAPI()

    job = api.add_test_job(state=State.PENDING)
    api.add_job_exception(job.id, JobError("test"))

    run.handle_pending_job_api(job, api)

    assert job.id in api.jobs_run
    assert job.id in api.jobs_cleaned

    assert job.state == State.FAILED
    assert job.status_message == "JobError: test"
    assert job.status_code is None


def test_handle_pending_job_run_exception(db):
    api = TestJobAPI()

    job = api.add_test_job(state=State.PENDING)
    api.add_job_exception(job.id, Exception("test"))

    with pytest.raises(Exception):
        run.handle_pending_job_api(job, api)

    assert job.id in api.jobs_run
    # we don't clean up on unknown exceptions
    assert job.id not in api.jobs_cleaned

    assert job.state == State.FAILED
    assert job.status_message == "Internal error when starting job"
    assert job.status_code is None


def test_handle_running_job_success(db):
    api = TestJobAPI()
    job = api.add_test_job(state=State.RUNNING)
    api.add_job_result(job.id, State.SUCCEEDED, None, "Finished")

    run.handle_running_job_api(job, api)

    assert job.status_message == "Finished"
    assert job.state == State.SUCCEEDED
    assert job.id in api.jobs_cleaned


def test_handle_running_job_still_running(db):
    api = TestJobAPI()
    job = api.add_test_job(state=State.RUNNING)
 
    run.handle_running_job_api(job, api)

    assert job.status_message == "Running"
    assert job.state == State.RUNNING


def test_handle_running_job_failed(db):
    api = TestJobAPI()
    job = api.add_test_job(state=State.RUNNING)
    api.add_job_result(
            job.id, 
            State.FAILED, 
            StatusCode.NONZERO_EXIT,
            "Job exited with an error code",
    )
 
    run.handle_running_job_api(job, api)

    assert job.state == State.FAILED
    assert job.status_code == StatusCode.NONZERO_EXIT
    assert job.status_message == "Job exited with an error code"
    assert job.id in api.jobs_cleaned


def test_handle_running_job_joberror(db):
    api = TestJobAPI()
    job = api.add_test_job(state=State.RUNNING)
    api.add_job_exception(job.id, JobError("job error"))
 
    run.handle_running_job_api(job, api)

    assert job.state == State.FAILED
    assert job.status_code is None
    assert job.status_message == "JobError: job error"
    assert job.id in api.jobs_cleaned


def test_handle_running_job_exception(db):
    api = TestJobAPI()
    job = api.add_test_job(state=State.RUNNING)
    api.add_job_exception(job.id, Exception("unknown error"))
 
    with pytest.raises(Exception):
        run.handle_running_job_api(job, api)

    assert job.state == State.FAILED
    assert job.status_code is None
    assert job.status_message == "Internal error when finalising job"
    assert job.id not in api.jobs_cleaned
