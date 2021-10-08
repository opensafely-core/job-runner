import pytest

from jobrunner import config, run
from jobrunner.job_executor import ExecutorState
from jobrunner.models import State, StatusCode
from tests.factories import StubJobAPI


@pytest.fixture()
def db(monkeypatch):
    """Create a throwaway db."""
    monkeypatch.setattr(
        config, "DATABASE_FILE", ":memory:{random.randrange(sys.maxsize)}"
    )


def test_handle_pending_job_cancelled(db):
    api = StubJobAPI()
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
    api = StubJobAPI()
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
    api = StubJobAPI()
    job = api.add_test_job(ExecutorState.UNKNOWN, State.PENDING)

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["prepare"]
    assert api.get_status(job).state == ExecutorState.PREPARING

    # our state
    assert job.status_message == "Preparing"
    assert job.state == State.RUNNING


def test_handle_job_pending_dependency_failed(db):
    api = StubJobAPI()
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
    api = StubJobAPI()
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
    api = StubJobAPI()
    job = api.add_test_job(exec_state, job_state)
    api.set_job_transition(job, exec_state)

    run.handle_job_api(job, api)

    assert job.id in api.tracker[tracker]
    assert api.get_status(job).state == exec_state

    assert job.state == job_state
    assert job.status_message == "Waiting on available resources"
    assert job.status_code == StatusCode.WAITING_ON_WORKERS


def test_handle_job_pending_to_error(db):
    api = StubJobAPI()

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
    api = StubJobAPI()
    job = api.add_test_job(ExecutorState.PREPARED, State.RUNNING)

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["execute"]
    assert api.get_status(job).state == ExecutorState.EXECUTING

    # our state
    assert job.state == State.RUNNING
    assert job.status_message == "Executing"


def test_handle_job_executed_to_finalizing(db):
    api = StubJobAPI()
    job = api.add_test_job(ExecutorState.EXECUTED, State.RUNNING)

    run.handle_job_api(job, api)

    # executor state
    assert job.id in api.tracker["finalize"]
    assert api.get_status(job).state == ExecutorState.FINALIZING

    # our state
    assert job.state == State.RUNNING
    assert job.status_message == "Finalizing"


def test_handle_job_finalized_success(db):
    api = StubJobAPI()
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


def test_handle_job_finalized_failed_exit_code(db):
    api = StubJobAPI()
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
    assert job.status_message == "Job exited with an error code"
    assert job.outputs == {"output/file.csv": "medium"}


def test_handle_job_finalized_failed_unmatched(db):
    api = StubJobAPI()
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
    api = StubJobAPI()
    job = api.add_test_job(
        current,
        State.PENDING if current == ExecutorState.UNKNOWN else State.RUNNING,
    )
    # this will cause any call to prepare/execute/finalize to return that state
    api.set_job_transition(job, invalid)

    with pytest.raises(run.InvalidTransition):
        run.handle_job_api(job, api)
