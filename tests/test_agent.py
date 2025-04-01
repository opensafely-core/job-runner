import time

import pytest
from opentelemetry import trace

from jobrunner import config, run
from jobrunner.job_executor import ExecutorState
from jobrunner.models import State, StatusCode
from tests.conftest import get_trace
from tests.factories import StubExecutorAPI


def test_handle_job_full_execution_async(db, freezer):
    # move to a whole second boundary for easier timestamp maths
    freezer.move_to("2022-01-01T12:34:56")

    api = StubExecutorAPI()

    start = int(time.time() * 1e9)

    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING, StatusCode.CREATED)

    freezer.tick(1)

    run.handle_job(job, api)
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.PREPARING

    freezer.tick(1)
    api.set_job_status_from_executor_state(job, ExecutorState.PREPARED)

    freezer.tick(1)
    run.handle_job(job, api)
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.EXECUTING

    freezer.tick(1)
    api.set_job_status_from_executor_state(job, ExecutorState.EXECUTED)

    freezer.tick(1)
    run.handle_job(job, api)
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.FINALIZING

    freezer.tick(1)
    api.set_job_status_from_executor_state(job, ExecutorState.FINALIZED)
    api.set_job_result(job)

    freezer.tick(1)
    run.handle_job(job, api)
    assert job.state == State.SUCCEEDED
    assert job.status_code == StatusCode.SUCCEEDED

    spans = get_trace("jobs")
    assert [s.name for s in spans] == [
        "CREATED",
        "PREPARING",
        "PREPARED",
        "EXECUTING",
        "EXECUTED",
        "FINALIZING",
        "FINALIZED",
        "SUCCEEDED",
        "JOB",
    ]

    span_times = [
        (s.name, (s.start_time - start) / 1e9, (s.end_time - start) / 1e9)
        for s in spans[:-1]
        if not s.name.startswith("ENTER")
    ]
    assert span_times == [
        ("CREATED", 0.0, 1.0),
        ("PREPARING", 1.0, 2.0),
        ("PREPARED", 2.0, 3.0),
        ("EXECUTING", 3.0, 4.0),
        ("EXECUTED", 4.0, 5.0),
        ("FINALIZING", 5.0, 6.0),
        ("FINALIZED", 6.0, 7.0),
        ("SUCCEEDED", 7.0, 8.0),  # this is always 1 second anyway!
    ]


def test_handle_job_full_execution_synchronous(db, freezer):
    # move to a whole second boundary for easier timestamp maths
    freezer.move_to("2022-01-01T12:34:56")

    api = StubExecutorAPI()
    api.synchronous_transitions = [ExecutorState.PREPARING, ExecutorState.FINALIZING]

    start = int(time.time_ns())

    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING, StatusCode.CREATED)

    freezer.tick(1)

    # prepare is synchronous
    api.set_job_transition(job, ExecutorState.PREPARED, hook=lambda j: freezer.tick(1))
    run.handle_job(job, api)
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.PREPARED

    freezer.tick(1)
    run.handle_job(job, api)
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.EXECUTING

    freezer.tick(1)
    api.set_job_status_from_executor_state(job, ExecutorState.EXECUTED)

    freezer.tick(1)
    # finalize is synchronous

    def finalize(job):
        freezer.tick(1)
        api.set_job_result(job)

    api.set_job_transition(job, ExecutorState.FINALIZED, hook=finalize)
    assert job.id not in api.tracker["finalize"]
    run.handle_job(job, api)
    assert job.id in api.tracker["finalize"]
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.FINALIZED

    freezer.tick(1)
    run.handle_job(job, api)
    assert job.state == State.SUCCEEDED
    assert job.status_code == StatusCode.SUCCEEDED

    spans = get_trace("jobs")
    assert [s.name for s in spans] == [
        "CREATED",
        "PREPARING",
        "PREPARED",
        "EXECUTING",
        "EXECUTED",
        "FINALIZING",
        "FINALIZED",
        "SUCCEEDED",
        "JOB",
    ]

    span_times = [
        (s.name, (s.start_time - start) / 1e9, (s.end_time - start) / 1e9)
        for s in spans[:-1]
        if not s.name.startswith("ENTER")
    ]
    assert span_times == [
        ("CREATED", 0.0, 1.0),
        ("PREPARING", 1.0, 2.0),
        ("PREPARED", 2.0, 3.0),
        ("EXECUTING", 3.0, 4.0),
        ("EXECUTED", 4.0, 5.0),
        ("FINALIZING", 5.0, 6.0),
        ("FINALIZED", 6.0, 7.0),
        ("SUCCEEDED", 7.0, 8.0),  # this is always 1 second anyway!
    ]


def test_handle_prepared_job_cancelled(db, monkeypatch):
    api = StubExecutorAPI()

    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING, StatusCode.CREATED)

    assert job.id not in api.tracker["prepare"]
    run.handle_job(job, api)
    assert job.id in api.tracker["prepare"]
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.PREPARING

    api.set_job_status_from_executor_state(job, ExecutorState.PREPARED)

    job.cancelled = True

    run.handle_job(job, api)

    # executor state
    job_definition = run.job_to_job_definition(job)

    # StubExecutorAPI needs state setting to FINALIZED, local executor is able to
    # determine this for itself based on the presence of volume & absence of container
    api.set_job_status_from_executor_state(job, ExecutorState.FINALIZED)

    # put this here for completeness so that we can compare to other executors
    assert api.get_status(job_definition).state == ExecutorState.FINALIZED
    assert job.status_code == StatusCode.FINALIZED
    assert job.state == State.RUNNING

    assert job.id not in api.tracker["cleanup"]
    run.handle_job(job, api)
    assert job.id in api.tracker["cleanup"]

    assert job.id in api.tracker["prepare"]
    assert job.id not in api.tracker["terminate"]
    assert job.id not in api.tracker["finalize"]
    assert job.id in api.tracker["cleanup"]

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "Cancelled by user"
    assert job.status_code == StatusCode.CANCELLED_BY_USER


def test_handle_running_job_cancelled(db, monkeypatch):
    api = StubExecutorAPI()

    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING, StatusCode.CREATED)

    assert job.id not in api.tracker["prepare"]
    run.handle_job(job, api)
    assert job.id in api.tracker["prepare"]
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.PREPARING

    api.set_job_status_from_executor_state(job, ExecutorState.PREPARED)

    assert job.id not in api.tracker["execute"]
    run.handle_job(job, api)
    assert job.id in api.tracker["execute"]
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.EXECUTING

    job.cancelled = True

    assert job.id not in api.tracker["terminate"]
    run.handle_job(job, api)
    assert job.id in api.tracker["terminate"]

    # executor state
    job_definition = run.job_to_job_definition(job)
    assert api.get_status(job_definition).state == ExecutorState.EXECUTED

    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.EXECUTED

    assert job.id not in api.tracker["finalize"]
    run.handle_job(job, api)
    assert job.id in api.tracker["finalize"]

    api.set_job_status_from_executor_state(job, ExecutorState.FINALIZED)

    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.FINALIZING

    assert job.id not in api.tracker["cleanup"]
    run.handle_job(job, api)
    assert job.id in api.tracker["cleanup"]

    assert job.state == State.FAILED
    assert job.status_message == "Cancelled by user"
    assert job.status_code == StatusCode.CANCELLED_BY_USER


@pytest.mark.parametrize(
    "executor_state,status_code,message",
    [
        (
            ExecutorState.PREPARING,
            StatusCode.PREPARING,
            "Preparing your code and workspace files",
        ),
        (ExecutorState.EXECUTING, StatusCode.EXECUTING, "Executing job on the backend"),
        (ExecutorState.FINALIZING, StatusCode.FINALIZING, "Recording job results"),
    ],
)
def test_handle_job_stable_states(executor_state, status_code, message, db):
    api = StubExecutorAPI()
    job = api.add_test_job(
        executor_state, State.RUNNING, status_code, status_message=message
    )

    run.handle_job(job, api)

    # executor state
    assert job.id not in api.tracker["prepare"]
    assert job.id not in api.tracker["execute"]
    assert job.id not in api.tracker["finalize"]
    assert api.get_status(job).state == executor_state

    # our state
    assert job.state == State.RUNNING
    assert job.status_message == message

    # no spans
    assert len(get_trace("jobs")) == 0


@pytest.mark.parametrize(
    "executor_state,job_state,status_code,tracker",
    [
        (ExecutorState.UNKNOWN, State.PENDING, StatusCode.CREATED, "prepare"),
        (ExecutorState.PREPARED, State.RUNNING, StatusCode.PREPARED, "execute"),
        (ExecutorState.EXECUTED, State.RUNNING, StatusCode.EXECUTED, "finalize"),
    ],
)
def test_handle_job_waiting_on_workers_via_executor(
    executor_state, job_state, status_code, tracker, db
):
    api = StubExecutorAPI()
    job = api.add_test_job(executor_state, job_state, status_code)
    api.set_job_transition(job, executor_state)

    run.handle_job(job, api)

    assert job.id in api.tracker[tracker]
    assert api.get_status(job).state == executor_state

    assert job.state == job_state
    assert job.status_message == "Waiting on available resources"
    assert job.status_code == StatusCode.WAITING_ON_WORKERS

    # tracing
    spans = get_trace("jobs")
    expected_trace_state = status_code.name
    assert spans[-1].name == expected_trace_state


def test_handle_job_prepared_to_executing(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.PREPARED, State.RUNNING, StatusCode.PREPARED)

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["execute"]
    assert api.get_status(job).state == ExecutorState.EXECUTING

    # our state
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.EXECUTING
    assert job.status_message == "Executing job on the backend"

    # tracing
    spans = get_trace("jobs")
    assert spans[-1].name == "PREPARED"


def test_handle_job_executed_to_finalizing(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.EXECUTED, State.RUNNING, StatusCode.EXECUTED)

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["finalize"]
    assert api.get_status(job).state == ExecutorState.FINALIZING

    # our state
    assert job.state == State.RUNNING
    assert job.status_message == "Recording job results"

    # tracing
    spans = get_trace("jobs")
    assert spans[-1].name == "EXECUTED"


def test_handle_running_db_maintenance_mode(db, backend_db_config):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.EXECUTING,
        State.RUNNING,
        StatusCode.EXECUTING,
        run_command="cohortextractor:latest generate_cohort",
        requires_db=True,
    )

    run.handle_job(job, api, mode="db-maintenance")

    # executor state
    assert job.id in api.tracker["terminate"]
    assert job.id in api.tracker["cleanup"]
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_DB_MAINTENANCE
    assert job.status_message == "Waiting for database to finish maintenance"
    assert job.started_at is None

    spans = get_trace("jobs")
    assert spans[-1].name == "EXECUTING"


def test_handle_running_cancelled_db_maintenance_mode(db, backend_db_config):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.EXECUTING,
        State.RUNNING,
        StatusCode.EXECUTING,
        run_command="cohortextractor:latest generate_cohort",
        requires_db=True,
        cancelled=True,
    )

    run.handle_job(job, api, mode="db-maintenance")

    # cancellation of running jobs puts it into EXECUTED for later finalization
    # executor state
    assert job.id in api.tracker["terminate"]
    assert api.get_status(job).state == ExecutorState.EXECUTED

    # our state
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.EXECUTED
    assert job.status_message == "Cancelled whilst executing"

    spans = get_trace("jobs")
    assert spans[-1].name == "EXECUTING"


@pytest.fixture
def backend_db_config(monkeypatch):
    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    # for test jobs, job.database_name is None, so add a dummy connection
    # string for that db
    monkeypatch.setitem(config.DATABASE_URLS, None, "conn str")


def invalid_transitions():
    """Enumerate all invalid transistions by inverting valid transitions"""

    def invalid(current, next_state):
        # the only valid transitions are:
        # - no transition
        # - the next state
        # - error
        valid = (current, next_state, ExecutorState.ERROR)
        for state in list(ExecutorState):
            if state not in valid:
                # this is an invalid transition
                yield current, state

    yield from invalid(ExecutorState.UNKNOWN, ExecutorState.PREPARING)
    yield from invalid(ExecutorState.PREPARED, ExecutorState.EXECUTING)
    yield from invalid(ExecutorState.EXECUTED, ExecutorState.FINALIZING)


@pytest.mark.parametrize("current, invalid", invalid_transitions())
def test_bad_transition(current, invalid, db):
    api = StubExecutorAPI()
    job = api.add_test_job(
        current,
        State.PENDING if current == ExecutorState.UNKNOWN else State.RUNNING,
    )
    # this will cause any call to prepare/execute/finalize to return that state
    api.set_job_transition(job, invalid)

    with pytest.raises(run.InvalidTransition):
        run.handle_job(job, api)


def test_handle_single_job_marks_as_failed(db, monkeypatch):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.EXECUTED, State.RUNNING, StatusCode.EXECUTED)

    def error(*args, **kwargs):
        raise Exception("test")

    monkeypatch.setattr(api, "get_status", error)

    with pytest.raises(Exception):
        run.handle_single_job(job, api)

    assert job.state is State.FAILED

    spans = get_trace("jobs")
    assert spans[-3].name == "EXECUTED"
    error_span = spans[-2]
    assert error_span.name == "INTERNAL_ERROR"
    assert error_span.status.status_code == trace.StatusCode.ERROR
    assert error_span.events[0].name == "exception"
    assert error_span.events[0].attributes["exception.message"] == "test"
    assert spans[-1].name == "JOB"

    spans = get_trace("loop")
    assert len(spans) == 1
    assert spans[0].name == "LOOP_JOB"
    assert spans[0].attributes["job"] == job.id
    assert spans[0].attributes["workspace"] == job.workspace
    assert spans[0].attributes["user"] == job._job_request["created_by"]
    assert spans[0].attributes["initial_code"] == "EXECUTED"
    assert spans[0].attributes["initial_state"] == "RUNNING"
    assert "final_code" not in spans[0].attributes
    assert "final_state" not in spans[0].attributes


def test_handle_single_job_with_executor_retry(db, monkeypatch):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.EXECUTED, State.RUNNING, StatusCode.EXECUTED)

    def retry(*args, **kwargs):
        raise run.ExecutorRetry("retry message")

    monkeypatch.setattr(api, "get_status", retry)

    run.handle_single_job(job, api)
    run.handle_single_job(job, api)

    assert job.state is State.RUNNING
    assert run.EXECUTOR_RETRIES[job.id] == 2

    spans = get_trace("loop")
    assert len(spans) == 2
    assert spans[0].attributes["executor_retry"] is True
    assert spans[0].attributes["executor_retry_message"] == "retry message"
    assert spans[0].attributes["executor_retry_count"] == 1

    assert spans[1].attributes["executor_retry"] is True
    assert spans[1].attributes["executor_retry_message"] == "retry message"
    assert spans[1].attributes["executor_retry_count"] == 2


def test_handle_single_job_shortcuts_synchronous(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING, StatusCode.CREATED)

    api.synchronous_transitions = [ExecutorState.PREPARING]

    run.handle_single_job(job, api)

    # executor state
    assert job.id in api.tracker["prepare"]
    assert job.id in api.tracker["execute"]
    assert api.get_status(job).state == ExecutorState.EXECUTING

    # our state
    assert job.status_message == "Executing job on the backend"
    assert job.state == State.RUNNING
    assert job.started_at

    # tracing
    assert [s.name for s in get_trace("jobs")] == [
        "CREATED",
        "PREPARING",
        "PREPARED",
    ]
