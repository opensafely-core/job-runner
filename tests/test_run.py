import pytest

from jobrunner import run
from jobrunner.job_executor import ExecutorState, JobStatus, Privacy
from jobrunner.models import State, StatusCode
from tests.factories import StubExecutorAPI, job_factory
from tests.fakes import RecordingExecutor


def test_handle_pending_job_cancelled(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING, cancelled=True)

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["terminate"]
    assert job.id in api.tracker["cleanup"]
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "Cancelled by user"
    assert job.status_code == StatusCode.CANCELLED_BY_USER


@pytest.mark.parametrize(
    "state,message",
    [
        (ExecutorState.PREPARING, "Preparing"),
        (ExecutorState.EXECUTING, "Executing"),
        (ExecutorState.FINALIZING, "Finalizing"),
    ],
)
def test_handle_job_stable_states(state, message, db):
    api = StubExecutorAPI()
    job = api.add_test_job(state, State.RUNNING)

    run.handle_job_api(job, api)

    # executor state
    assert job.id not in api.tracker["prepare"]
    assert job.id not in api.tracker["execute"]
    assert job.id not in api.tracker["finalize"]
    assert api.get_status(job).state == state

    # our state
    assert job.state == State.RUNNING
    assert job.status_message == message


def test_handle_job_pending_to_preparing(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING)

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["prepare"]
    assert api.get_status(job).state == ExecutorState.PREPARING

    # our state
    assert job.status_message == "Preparing"
    assert job.state == State.RUNNING
    assert job.started_at


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

    run.handle_job_api(job, api)

    # executor state
    assert job.id not in api.tracker["prepare"]
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "Not starting as dependency failed"
    assert job.status_code == StatusCode.DEPENDENCY_FAILED


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

    run.handle_job_api(job, api)

    # executor state
    assert job.id not in api.tracker["prepare"]
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.PENDING
    assert job.status_message == "Waiting on dependencies"
    assert job.status_code == StatusCode.WAITING_ON_DEPENDENCIES


@pytest.mark.parametrize(
    "exec_state,job_state,tracker",
    [
        (ExecutorState.UNKNOWN, State.PENDING, "prepare"),
        (ExecutorState.PREPARED, State.RUNNING, "execute"),
        (ExecutorState.EXECUTED, State.RUNNING, "finalize"),
    ],
)
def test_handle_job_waiting_on_workers(exec_state, job_state, tracker, db):
    api = StubExecutorAPI()
    job = api.add_test_job(exec_state, job_state)
    api.set_job_transition(job, exec_state)

    run.handle_job_api(job, api)

    assert job.id in api.tracker[tracker]
    assert api.get_status(job).state == exec_state

    assert job.state == job_state
    assert job.status_message == "Waiting on available resources"
    assert job.status_code == StatusCode.WAITING_ON_WORKERS


def test_handle_job_pending_to_error(db):
    api = StubExecutorAPI()

    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING)
    api.set_job_transition(job, ExecutorState.ERROR, "it is b0rked")

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["prepare"]
    assert job.id in api.tracker["cleanup"]
    # its been cleaned up and is now unknown
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "it is b0rked"
    assert job.status_code is None


def test_handle_job_prepared_to_executing(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.PREPARED, State.RUNNING)

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["execute"]
    assert api.get_status(job).state == ExecutorState.EXECUTING

    # our state
    assert job.state == State.RUNNING
    assert job.status_message == "Executing"


def test_handle_job_executed_to_finalizing(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.EXECUTED, State.RUNNING)

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["finalize"]
    assert api.get_status(job).state == ExecutorState.FINALIZING

    # our state
    assert job.state == State.RUNNING
    assert job.status_message == "Finalizing"


def test_handle_job_finalized_success_with_delete(db):
    api = StubExecutorAPI()

    # insert previous outputs
    job_factory(
        state=State.SUCCEEDED,
        outputs={"output/old.csv": "medium"},
    )

    job = api.add_test_job(ExecutorState.FINALIZED, State.RUNNING)
    api.set_job_result(job, {"output/file.csv": "medium"})

    run.handle_job_api(job, api)

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


def test_handle_job_finalized_failed_exit_code(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.FINALIZED, State.RUNNING)
    api.set_job_result(job, {"output/file.csv": "medium"}, exit_code=1)

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["cleanup"]
    # its been cleaned up and is now unknown
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_code == StatusCode.NONZERO_EXIT
    assert job.status_message == "Job exited with error code 1"
    assert job.outputs == {"output/file.csv": "medium"}


def test_handle_job_finalized_failed_unmatched(db):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.FINALIZED, State.RUNNING)
    api.set_job_result(job, {"output/file.csv": "medium"}, unmatched=["badfile.csv"])

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["cleanup"]
    # its been cleaned up and is now unknown
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "No outputs found matching patterns:\n - badfile.csv"
    assert job.outputs == {"output/file.csv": "medium"}


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
        run.handle_job_api(job, api)


def test_handle_active_job_marks_as_failed(db, monkeypatch):
    api = StubExecutorAPI()
    job = api.add_test_job(ExecutorState.EXECUTED, State.RUNNING)

    def error(*args, **kwargs):
        raise Exception("test")

    monkeypatch.setattr(api, "get_status", error)

    with pytest.raises(Exception):
        run.handle_active_job_api(job, api)

    assert job.state is State.FAILED


def test_ignores_cancelled_jobs_when_calculating_dependencies(db):
    job_factory(
        id="1",
        action="other-action",
        state=State.SUCCEEDED,
        created_at=1000,
        outputs={"output-from-completed-run": "highly_sensitive_output"},
    )
    job_factory(
        id="2",
        action="other-action",
        state=State.SUCCEEDED,
        created_at=2000,
        cancelled=True,
        outputs={"output-from-cancelled-run": "highly_sensitive_output"},
    )

    api = RecordingExecutor(
        JobStatus(ExecutorState.UNKNOWN), JobStatus(ExecutorState.PREPARING)
    )
    run.handle_job_api(
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
