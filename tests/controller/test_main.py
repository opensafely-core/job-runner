import time

import pytest
from opentelemetry import trace

from jobrunner import config, run
from jobrunner.job_executor import ExecutorState, JobStatus
from jobrunner.models import State, StatusCode
from tests.conftest import get_trace
from tests.factories import StubExecutorAPI, job_factory
from tests.fakes import RecordingExecutor


def test_handle_pending_job_cancelled(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING, cancelled=True)

    assert job.id not in api.tracker["prepare"]
    assert job.id not in api.tracker["terminate"]
    assert job.id not in api.tracker["finalize"]
    assert job.id not in api.tracker["cleanup"]

    run.handle_job(job, api)

    assert job.id not in api.tracker["prepare"]
    assert job.id not in api.tracker["terminate"]
    assert job.id not in api.tracker["finalize"]
    assert job.id not in api.tracker["cleanup"]

    # executor state
    job_definition = run.job_to_job_definition(job)
    assert api.get_status(job_definition).state == ExecutorState.UNKNOWN

    # run.handle_job(job, api)

    # assert job.id not in api.tracker["prepare"]
    # assert job.id in api.tracker["terminate"]
    # assert job.id not in api.tracker["finalize"]
    # assert job.id not in api.tracker["cleanup"]

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "Cancelled by user"
    assert job.status_code == StatusCode.CANCELLED_BY_USER


def test_handle_job_pending_dependency_failed(db):
    api = StubExecutorAPI()
    dependency = api.add_test_job(ExecutorState.UNKNOWN, State.FAILED)
    job = api.add_test_job(
        ExecutorState.UNKNOWN,
        State.PENDING,
        job_request_id=dependency.job_request_id,
        action="action2",
        wait_for_job_ids=[dependency.id],
    )

    run.handle_job(job, api)

    # executor state
    assert job.id not in api.tracker["prepare"]
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "Not starting as dependency failed"
    assert job.status_code == StatusCode.DEPENDENCY_FAILED

    # tracing
    spans = get_trace("jobs")
    assert spans[-3].name == "CREATED"
    assert spans[-2].name == "DEPENDENCY_FAILED"
    assert spans[-2].status.status_code == trace.StatusCode.ERROR
    assert spans[-1].name == "JOB"
    assert spans[-1].status.status_code == trace.StatusCode.ERROR


def test_handle_pending_job_waiting_on_dependency(db):
    api = StubExecutorAPI()
    dependency = api.add_test_job(ExecutorState.EXECUTING, State.RUNNING)

    job = api.add_test_job(
        ExecutorState.UNKNOWN,
        State.PENDING,
        job_request_id=dependency.job_request_id,
        action="action2",
        wait_for_job_ids=[dependency.id],
    )

    run.handle_job(job, api)

    # executor state
    assert job.id not in api.tracker["prepare"]
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.PENDING
    assert job.status_message == "Waiting on dependencies"
    assert job.status_code == StatusCode.WAITING_ON_DEPENDENCIES

    # tracing
    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_job_waiting_on_workers(monkeypatch, db):
    monkeypatch.setattr(config, "MAX_WORKERS", 0)
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING)

    run.handle_job(job, api)

    # executor doesn't even know about it
    assert job.id not in api.tracker["prepare"]

    assert job.state == State.PENDING
    assert job.status_message == "Waiting on available workers"
    assert job.status_code == StatusCode.WAITING_ON_WORKERS

    # tracing
    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_job_waiting_on_db_workers(monkeypatch, db):
    monkeypatch.setattr(config, "MAX_DB_WORKERS", 0)
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.UNKNOWN,
        State.PENDING,
        run_command="cohortextractor:latest generate_cohort",
        requires_db=True,
    )

    run.handle_job(job, api)

    # executor doesn't even know about it
    assert job.id not in api.tracker["prepare"]

    assert job.state == State.PENDING
    assert job.status_message == "Waiting on available database workers"
    assert job.status_code == StatusCode.WAITING_ON_DB_WORKERS

    # tracing
    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_job_finalized_success_with_large_file(db):
    api = StubExecutorAPI()

    # insert previous outputs
    job_factory(
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        outputs={"output/output.csv": "moderately_sensitive"},
    )

    job = api.add_test_job(ExecutorState.FINALIZED, State.RUNNING, StatusCode.FINALIZED)
    api.set_job_result(
        job,
        outputs={"output/output.csv": "moderately_sensitive"},
        level4_excluded_files={"output/output.csv": "too big"},
    )

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["cleanup"]
    # its been cleaned up and is now unknown
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.SUCCEEDED
    assert "Completed successfully" in job.status_message
    assert "were excluded" in job.status_message
    assert "output/output.csv: too big" not in job.status_message


@pytest.mark.parametrize(
    "exit_code,run_command,extra_message",
    [
        (
            3,
            "cohortextractor generate_cohort",
            (
                "A transient database error occurred, your job may run "
                "if you try it again, if it keeps failing then contact tech support"
            ),
        ),
        (
            4,
            "cohortextractor generate_cohort",
            "New data is being imported into the database, please try again in a few hours",
        ),
        (
            5,
            "cohortextractor generate_cohort",
            "Something went wrong with the database, please contact tech support",
        ),
        # the same exit codes for a job that doesn't have access to the database show no message
        (3, "python foo.py", None),
        (4, "python foo.py", None),
        (5, "python foo.py", None),
    ],
)
def test_handle_job_finalized_failed_exit_code(
    exit_code, run_command, extra_message, db, backend_db_config
):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.FINALIZED,
        State.RUNNING,
        StatusCode.FINALIZED,
        run_command=run_command,
        requires_db="cohortextractor" in run_command,
    )
    api.set_job_result(
        job,
        outputs={"output/file.csv": "highly_sensitive"},
        exit_code=exit_code,
        message=None,
    )

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["cleanup"]
    # its been cleaned up and is now unknown
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_code == StatusCode.NONZERO_EXIT
    expected = "Job exited with an error"
    if extra_message:
        expected += f": {extra_message}"
    assert job.status_message == expected
    assert job.outputs == {"output/file.csv": "highly_sensitive"}

    spans = get_trace("jobs")
    assert spans[-3].name == "FINALIZED"
    completed_span = spans[-2]
    assert completed_span.name == "NONZERO_EXIT"
    assert completed_span.attributes["exit_code"] == exit_code
    assert completed_span.attributes["outputs"] == 1
    assert completed_span.attributes["unmatched_patterns"] == 0
    assert completed_span.attributes["unmatched_outputs"] == 0
    assert completed_span.attributes["image_id"] == "image_id"
    assert completed_span.status.status_code == trace.StatusCode.ERROR
    assert spans[-1].name == "JOB"


def test_handle_job_finalized_failed_unmatched_patterns(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.FINALIZED, State.RUNNING, StatusCode.FINALIZED)
    api.set_job_result(
        job,
        outputs={"output/file.csv": "highly_sensitive"},
        unmatched_patterns=["badfile.csv"],
        unmatched_outputs=["otherbadfile.csv"],
    )

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["cleanup"]
    # its been cleaned up and is now unknown
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "No outputs found matching patterns:\n - badfile.csv"
    assert job.outputs == {"output/file.csv": "highly_sensitive"}
    assert job.unmatched_outputs == ["otherbadfile.csv"]

    spans = get_trace("jobs")
    assert spans[-3].name == "FINALIZED"
    completed_span = spans[-2]
    assert completed_span.name == "UNMATCHED_PATTERNS"
    assert completed_span.attributes["outputs"] == 1
    assert completed_span.attributes["unmatched_patterns"] == 1
    assert completed_span.attributes["unmatched_outputs"] == 1
    assert spans[-1].name == "JOB"


@pytest.fixture
def backend_db_config(monkeypatch):
    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    # for test jobs, job.database_name is None, so add a dummy connection
    # string for that db
    monkeypatch.setitem(config.DATABASE_URLS, None, "conn str")


def test_handle_pending_db_maintenance_mode(db, backend_db_config):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.UNKNOWN,
        State.PENDING,
        run_command="cohortextractor:latest generate_cohort",
        requires_db=True,
    )

    run.handle_job(job, api, mode="db-maintenance")

    # executor state
    assert api.get_status(job).state == ExecutorState.UNKNOWN
    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_DB_MAINTENANCE
    assert job.status_message == "Waiting for database to finish maintenance"
    assert job.started_at is None

    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_pending_cancelled_db_maintenance_mode(db, backend_db_config):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.UNKNOWN,
        State.PENDING,
        run_command="cohortextractor:latest generate_cohort",
        requires_db=True,
        cancelled=True,
    )

    run.handle_job(job, api, mode="db-maintenance")

    # executor state
    assert api.get_status(job).state == ExecutorState.UNKNOWN
    # our state
    assert job.state == State.FAILED
    assert job.status_code == StatusCode.CANCELLED_BY_USER
    assert job.status_message == "Cancelled by user"
    assert job.started_at is None


def test_handle_pending_pause_mode(db, backend_db_config):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.UNKNOWN,
        State.PENDING,
        run_command="cohortextractor:latest generate_cohort",
        requires_db=True,
    )

    run.handle_job(job, api, paused=True)

    # executor state
    assert api.get_status(job).state == ExecutorState.UNKNOWN
    # our state
    assert job.state == State.PENDING
    assert job.started_at is None
    assert "paused" in job.status_message

    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_running_pause_mode(db, backend_db_config):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.EXECUTING,
        State.RUNNING,
        StatusCode.EXECUTING,
        status_message="doing my thang",
        run_command="cohortextractor:latest generate_cohort",
        requires_db=True,
    )

    run.handle_job(job, api, paused=True)

    # check we did nothing
    # executor state
    assert api.get_status(job).state == ExecutorState.EXECUTING
    # our state
    assert job.state == State.RUNNING
    assert "paused" not in job.status_message

    spans = get_trace("jobs")
    assert len(spans) == 0  # no spans


def test_ignores_cancelled_jobs_when_calculating_dependencies(db):
    job_factory(
        id="1",
        action="other-action",
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        created_at=1000,
        outputs={"output-from-completed-run": "highly_sensitive_output"},
    )
    job_factory(
        id="2",
        action="other-action",
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        created_at=2000,
        cancelled=True,
        outputs={"output-from-cancelled-run": "highly_sensitive_output"},
    )

    api = RecordingExecutor(
        JobStatus(ExecutorState.UNKNOWN), JobStatus(ExecutorState.PREPARING)
    )
    run.handle_job(
        job_factory(
            id="3", requires_outputs_from=["other-action"], state=State.PENDING
        ),
        api,
    )

    assert api.job_definition.inputs == ["output-from-completed-run"]


def test_get_obsolete_files_nothing_to_delete(db):
    outputs = {
        "high.txt": "highly_sensitive",
        "medium.txt": "moderately_sensitive",
    }
    job_factory(
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        outputs={},
        created_at=time.time() - 10,
    )

    job = job_factory(
        state=State.RUNNING,
        status_code=StatusCode.FINALIZED,
        outputs=outputs,
    )

    job_definition = run.job_to_job_definition(job)

    obsolete = run.get_obsolete_files(job_definition, outputs)
    assert obsolete == []


@pytest.mark.xfail
def test_get_obsolete_files_things_to_delete(db):
    old_outputs = {
        "old_high.txt": "highly_sensitive",
        "old_medium.txt": "moderately_sensitive",
        "current.txt": "highly_sensitive",
    }
    new_outputs = {
        "new_high.txt": "highly_sensitive",
        "new_medium.txt": "moderately_sensitive",
        "current.txt": "highly_sensitive",
    }
    job_factory(
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        outputs=old_outputs,
        created_at=time.time() - 10,
    )

    job = job_factory(
        state=State.RUNNING,
        status_code=StatusCode.FINALIZED,
        outputs=new_outputs,
    )

    job_definition = run.job_to_job_definition(job)

    obsolete = run.get_obsolete_files(job_definition, new_outputs)
    assert obsolete == ["old_high.txt", "old_medium.txt"]


@pytest.mark.xfail
def test_get_obsolete_files_things_to_delete_timing(db):
    old_outputs = {
        "old_high.txt": "highly_sensitive",
        "old_medium.txt": "moderately_sensitive",
        "current.txt": "highly_sensitive",
    }
    new_outputs = {
        "new_high.txt": "highly_sensitive",
        "new_medium.txt": "moderately_sensitive",
        "current.txt": "highly_sensitive",
    }

    # insert previous outputs
    job_factory(
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        outputs=old_outputs,
        created_at=time.time() - 10,
    )

    job = job_factory(
        state=State.RUNNING,
        status_code=StatusCode.FINALIZED,
        outputs=new_outputs,
    )

    job_definition = run.job_to_job_definition(job)

    obsolete = run.get_obsolete_files(job_definition, new_outputs)
    assert obsolete == ["old_high.txt", "old_medium.txt"]


def test_get_obsolete_files_case_change(db):
    old_outputs = {
        "high.txt": "highly_sensitive",
    }
    new_outputs = {
        "HIGH.txt": "highly_sensitive",
    }
    job_factory(
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        outputs=old_outputs,
        created_at=time.time() - 10,
    )

    job = job_factory(
        state=State.RUNNING,
        status_code=StatusCode.FINALIZED,
        outputs=new_outputs,
    )

    job_definition = run.job_to_job_definition(job)

    obsolete = run.get_obsolete_files(job_definition, new_outputs)
    assert obsolete == []


def test_job_definition_limits(db):
    job = job_factory()
    job_definition = run.job_to_job_definition(job)
    assert job_definition.cpu_count == 2
    assert job_definition.memory_limit == "4G"


def test_mark_job_as_failed_adds_error(db):
    job = job_factory()
    run.mark_job_as_failed(job, StatusCode.INTERNAL_ERROR, "error")

    # tracing
    spans = get_trace("jobs")
    assert spans[-3].name == "CREATED"
    assert spans[-2].name == "INTERNAL_ERROR"
    assert spans[-2].status.status_code == trace.StatusCode.ERROR
    assert spans[-1].name == "JOB"
    assert spans[-1].status.status_code == trace.StatusCode.ERROR
