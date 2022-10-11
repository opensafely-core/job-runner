import pytest
from opentelemetry import trace

from jobrunner import config, run
from jobrunner.job_executor import ExecutorState, JobStatus, Privacy
from jobrunner.models import State, StatusCode
from tests.conftest import get_trace
from tests.factories import StubExecutorAPI, job_factory
from tests.fakes import RecordingExecutor


def test_handle_pending_job_cancelled(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING, cancelled=True)

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["terminate"]
    assert job.id in api.tracker["cleanup"]
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "Cancelled by user"
    assert job.status_code == StatusCode.CANCELLED_BY_USER


@pytest.mark.parametrize(
    "state,code,message",
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
def test_handle_job_stable_states(state, code, message, db):
    api = StubExecutorAPI()
    job = api.add_test_job(state, State.RUNNING, code, status_message=message)

    run.handle_job(job, api)

    # executor state
    assert job.id not in api.tracker["prepare"]
    assert job.id not in api.tracker["execute"]
    assert job.id not in api.tracker["finalize"]
    assert api.get_status(job).state == state

    # our state
    assert job.state == State.RUNNING
    assert job.status_message == message

    # no spans
    assert len(get_trace()) == 0


def test_handle_job_initial_error(db):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.ERROR, State.RUNNING, StatusCode.EXECUTING, message="broken"
    )

    # we raise the error to be handled in handle_single_job
    with pytest.raises(run.ExecutorError) as exc:
        run.handle_job(job, api)

    assert str(exc.value) == "broken"


def test_handle_job_pending_to_preparing(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING)

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["prepare"]
    assert api.get_status(job).state == ExecutorState.PREPARING

    # our state
    assert job.status_message == "Preparing your code and workspace files"
    assert job.state == State.RUNNING
    assert job.started_at

    # tracing
    spans = get_trace()
    assert spans[-2].name == "CREATED"
    assert spans[-1].name == "ENTER PREPARING"


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
    spans = get_trace()
    assert spans[-3].name == "CREATED"
    assert spans[-2].name == "DEPENDENCY_FAILED"
    assert spans[-1].name == "JOB"


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
    spans = get_trace()
    assert spans[-2].name == "CREATED"
    assert spans[-1].name == "ENTER WAITING_ON_DEPENDENCIES"


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
    spans = get_trace()
    assert spans[-2].name == "CREATED"
    assert spans[-1].name == "ENTER WAITING_ON_WORKERS"


@pytest.mark.parametrize(
    "exec_state,job_state,code,tracker",
    [
        (ExecutorState.UNKNOWN, State.PENDING, StatusCode.CREATED, "prepare"),
        (ExecutorState.PREPARED, State.RUNNING, StatusCode.PREPARED, "execute"),
        (ExecutorState.EXECUTED, State.RUNNING, StatusCode.EXECUTED, "finalize"),
    ],
)
def test_handle_job_waiting_on_workers_via_executor(
    exec_state, job_state, code, tracker, db
):
    api = StubExecutorAPI()
    job = api.add_test_job(exec_state, job_state, code)
    api.set_job_transition(job, exec_state)

    run.handle_job(job, api)

    assert job.id in api.tracker[tracker]
    assert api.get_status(job).state == exec_state

    assert job.state == job_state
    assert job.status_message == "Waiting on available resources"
    assert job.status_code == StatusCode.WAITING_ON_WORKERS

    # tracing
    spans = get_trace()
    expected_trace_state = code.name
    assert spans[-2].name == expected_trace_state
    assert spans[-1].name == "ENTER WAITING_ON_WORKERS"


def test_handle_job_pending_to_error(db):
    api = StubExecutorAPI()

    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING)
    api.set_job_transition(job, ExecutorState.ERROR, "it is b0rked")

    # we raise the error to be handled in handle_single_job
    with pytest.raises(run.ExecutorError) as exc:
        run.handle_job(job, api)

    assert str(exc.value) == "it is b0rked"


def test_handle_job_prepared_to_executing(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.PREPARED, State.RUNNING, StatusCode.PREPARED)

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["execute"]
    assert api.get_status(job).state == ExecutorState.EXECUTING

    # our state
    assert job.state == State.RUNNING
    assert job.status_message == "Executing job on the backend"

    # tracing
    spans = get_trace()
    assert spans[-2].name == "PREPARED"
    assert spans[-1].name == "ENTER EXECUTING"


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
    spans = get_trace()
    assert spans[-2].name == "EXECUTED"
    assert spans[-1].name == "ENTER FINALIZING"


def test_handle_job_finalized_success_with_delete(db):
    api = StubExecutorAPI()

    # insert previous outputs
    job_factory(
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        outputs={"output/old.csv": "medium"},
    )

    job = api.add_test_job(ExecutorState.FINALIZED, State.RUNNING, StatusCode.FINALIZED)
    api.set_job_result(job, outputs={"output/file.csv": "medium"})

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["cleanup"]
    # its been cleaned up and is now unknown
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.SUCCEEDED
    assert job.status_message == "Completed successfully"
    assert job.outputs == {"output/file.csv": "medium"}
    assert api.deleted["workspace"][Privacy.MEDIUM] == ["output/old.csv"]
    assert api.deleted["workspace"][Privacy.HIGH] == ["output/old.csv"]

    # tracing
    spans = get_trace()
    assert spans[-3].name == "FINALIZED"
    assert spans[-2].name == "SUCCEEDED"
    assert spans[-1].name == "JOB"


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
    )
    api.set_job_result(
        job, outputs={"output/file.csv": "medium"}, exit_code=exit_code, message=None
    )

    run.handle_job(job, api)

    # executor state
    assert job.id in api.tracker["cleanup"]
    # its been cleaned up and is now unknown
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_code == StatusCode.NONZERO_EXIT
    expected = f"Job exited with error code {exit_code}"
    if extra_message:
        expected += f": {extra_message}"
    assert job.status_message == expected
    assert job.outputs == {"output/file.csv": "medium"}

    spans = get_trace()
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
        outputs={"output/file.csv": "medium"},
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
    assert job.outputs == {"output/file.csv": "medium"}
    assert job.unmatched_outputs == ["otherbadfile.csv"]

    spans = get_trace()
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
    )

    run.handle_job(job, api, mode="db-maintenance")

    # executor state
    assert api.get_status(job).state == ExecutorState.UNKNOWN
    # our state
    assert job.state == State.PENDING
    assert job.status_message == "Waiting for database to finish maintenance"
    assert job.started_at is None

    spans = get_trace()
    assert spans[-2].name == "CREATED"
    assert spans[-1].name == "ENTER WAITING_DB_MAINTENANCE"


def test_handle_running_db_maintenance_mode(db, backend_db_config):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.EXECUTING,
        State.RUNNING,
        StatusCode.EXECUTING,
        run_command="cohortextractor:latest generate_cohort",
    )

    run.handle_job(job, api, mode="db-maintenance")

    # executor state
    assert job.id in api.tracker["terminate"]
    assert job.id in api.tracker["cleanup"]
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.PENDING
    assert job.status_message == "Waiting for database to finish maintenance"
    assert job.started_at is None

    spans = get_trace()
    assert spans[-2].name == "EXECUTING"
    assert spans[-1].name == "ENTER WAITING_DB_MAINTENANCE"


def test_handle_pending_pause_mode(db, backend_db_config):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.UNKNOWN,
        State.PENDING,
        run_command="cohortextractor:latest generate_cohort",
    )

    run.handle_job(job, api, paused=True)

    # executor state
    assert api.get_status(job).state == ExecutorState.UNKNOWN
    # our state
    assert job.state == State.PENDING
    assert job.started_at is None
    assert "paused" in job.status_message

    spans = get_trace()
    assert spans[-2].name == "CREATED"
    assert spans[-1].name == "ENTER WAITING_PAUSED"


def test_handle_running_pause_mode(db, backend_db_config):
    api = StubExecutorAPI()
    job = api.add_test_job(
        ExecutorState.EXECUTING,
        State.RUNNING,
        StatusCode.EXECUTING,
        status_message="doing my thang",
        run_command="cohortextractor:latest generate_cohort",
    )

    run.handle_job(job, api, paused=True)

    # check we did nothing
    # executor state
    assert api.get_status(job).state == ExecutorState.EXECUTING
    # our state
    assert job.state == State.RUNNING
    assert "paused" not in job.status_message

    spans = get_trace()
    assert len(spans) == 0  # no spans


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

    spans = get_trace()
    assert spans[-3].name == "EXECUTED"
    error_span = spans[-2]
    assert error_span.name == "INTERNAL_ERROR"
    assert error_span.status.status_code == trace.StatusCode.ERROR
    assert error_span.events[0].name == "exception"
    assert error_span.events[0].attributes["exception.message"] == "test"
    assert spans[-1].name == "JOB"


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

    assert api.job.inputs == ["output-from-completed-run"]


def test_get_obsolete_files_nothing_to_delete(db):

    outputs = {
        "high.txt": "high_privacy",
        "medium.txt": "medium_privacy",
    }
    job = job_factory(
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        outputs=outputs,
    )
    definition = run.job_to_job_definition(job)

    obsolete = run.get_obsolete_files(definition, outputs)
    assert obsolete == []


def test_get_obsolete_files_things_to_delete(db):

    old_outputs = {
        "old_high.txt": "high_privacy",
        "old_medium.txt": "medium_privacy",
        "current.txt": "high_privacy",
    }
    new_outputs = {
        "new_high.txt": "high_privacy",
        "new_medium.txt": "medium_privacy",
        "current.txt": "high_privacy",
    }
    job = job_factory(
        state=State.SUCCEEDED,
        outputs=old_outputs,
    )
    definition = run.job_to_job_definition(job)

    obsolete = run.get_obsolete_files(definition, new_outputs)
    assert obsolete == ["old_high.txt", "old_medium.txt"]


def test_get_obsolete_files_case_change(db):

    old_outputs = {
        "high.txt": "high_privacy",
    }
    new_outputs = {
        "HIGH.txt": "high_privacy",
    }
    job = job_factory(
        state=State.SUCCEEDED,
        outputs=old_outputs,
    )
    definition = run.job_to_job_definition(job)

    obsolete = run.get_obsolete_files(definition, new_outputs)
    assert obsolete == []


def test_job_definition_limits(db):
    job = job_factory()
    definition = run.job_to_job_definition(job)
    assert definition.cpu_count == 2
    assert definition.memory_limit == "4G"
