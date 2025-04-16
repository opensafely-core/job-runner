from unittest.mock import Mock, patch

import pytest

from jobrunner import config
from jobrunner.agent import main, task_api
from jobrunner.controller import task_api as controller_task_api
from jobrunner.job_executor import ExecutorState, JobDefinition
from tests.agent.stubs import StubExecutorAPI
from tests.conftest import get_trace


def test_handle_job_full_execution(db, freezer):
    # move to a whole second boundary for easier timestamp maths
    freezer.move_to("2022-01-01T12:34:56")

    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.UNKNOWN)

    freezer.tick(1)

    # prepare is synchronous
    api.set_job_transition(
        job_id, ExecutorState.PREPARED, hook=lambda j: freezer.tick(1)
    )
    main.handle_single_task(task, api)

    task = controller_task_api.get_task(task.id)
    assert task.agent_stage == ExecutorState.PREPARED.value

    freezer.tick(1)
    main.handle_single_task(task, api)
    task = controller_task_api.get_task(task.id)
    assert task.agent_stage == ExecutorState.EXECUTING.value

    freezer.tick(1)
    api.set_job_status(job_id, ExecutorState.EXECUTED)

    freezer.tick(1)
    # finalize is synchronous

    def finalize(job):
        freezer.tick(1)
        api.set_job_metadata(job.id)

    api.set_job_transition(job_id, ExecutorState.FINALIZED, hook=finalize)
    assert job_id not in api.tracker["finalize"]
    main.handle_single_task(task, api)
    assert job_id in api.tracker["finalize"]
    task = controller_task_api.get_task(task.id)
    assert task.agent_stage == ExecutorState.FINALIZED.value
    assert task.agent_complete
    assert "results" in task.agent_results

    spans = get_trace("agent_loop")
    # one span each time we called main.handle_single_task
    assert len(spans) == 3

    assert spans[0].attributes["initial_job_status"] == "UNKNOWN"
    assert spans[0].attributes["final_job_status"] == "PREPARED"
    assert not spans[0].attributes["complete"]
    assert spans[1].attributes["initial_job_status"] == "PREPARED"
    assert spans[1].attributes["final_job_status"] == "EXECUTING"
    assert not spans[1].attributes["complete"]
    assert spans[2].attributes["initial_job_status"] == "EXECUTED"
    assert spans[2].attributes["final_job_status"] == "FINALIZED"
    assert spans[2].attributes["complete"]


@pytest.mark.parametrize(
    "executor_state",
    [
        ExecutorState.ERROR,
        ExecutorState.EXECUTING,
        ExecutorState.FINALIZED,
    ],
)
def test_handle_job_stable_states(db, executor_state):
    api = StubExecutorAPI()
    task, job_id = api.add_test_runjob_task(executor_state)
    job = JobDefinition.from_dict(task.definition)

    with patch(
        "jobrunner.agent.task_api.update_controller", spec=task_api.update_controller
    ) as mock_update_controller:
        main.handle_single_task(task, api)

    # should be in the same state
    mock_update_controller.assert_called_with(
        task,
        executor_state.value,
        None,
        False if executor_state == ExecutorState.EXECUTING else True,
    )

    assert job.id not in api.tracker["prepare"]
    assert job.id not in api.tracker["execute"]
    assert job.id not in api.tracker["finalize"]

    # no spans
    assert len(get_trace("jobs")) == 0
    spans = get_trace("agent_loop")
    assert len(spans) == 1

    assert spans[0].attributes["initial_job_status"] == executor_state.name
    assert spans[0].attributes["final_job_status"] == executor_state.name


def test_handle_job_requires_db_has_secrets(db, monkeypatch):
    api = StubExecutorAPI()
    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    monkeypatch.setattr(config, "DATABASE_URLS", {None: "dburl"})

    task, job_id = api.add_test_runjob_task(ExecutorState.PREPARED, requires_db=True)

    def check_env(definition):
        assert definition.env["DATABASE_URL"] == "dburl"

    api.set_job_transition(job_id, ExecutorState.EXECUTING, hook=check_env)

    main.handle_run_job_task(task, api)


@patch("jobrunner.agent.task_api.update_controller")
def test_handle_job_with_error(mock_update_controller, db):
    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.UNKNOWN)

    api.set_job_transition(
        job_id, ExecutorState.PREPARED, hook=Mock(side_effect=Exception("foo"))
    )

    with pytest.raises(Exception):
        main.handle_single_task(task, api)

    assert mock_update_controller.called_with(
        task=task,
        stage=ExecutorState.ERROR,
        results={"error": "Exception('foo')"},
        complete=True,
    )

    spans = get_trace("agent_loop")
    assert len(spans) == 1
    span = spans[0]
    # update_controller is called with the error state outside of the
    # LOOP_TASK span, so final state is still PREPARING
    assert span.attributes["initial_job_status"] == "UNKNOWN"
    assert span.attributes["final_job_status"] == "PREPARING"
    # exception info has been added to the span
    assert span.status.status_code.name == "ERROR"
    assert span.status.description == "Exception: foo"


@pytest.mark.parametrize(
    "initial_state,terminate,finalize,cleanup",
    [
        (ExecutorState.PREPARED, False, True, False),
        (ExecutorState.EXECUTING, True, True, True),
        (ExecutorState.UNKNOWN, False, True, False),
        (ExecutorState.EXECUTED, False, True, True),
        (ExecutorState.ERROR, False, True, True),
        (ExecutorState.FINALIZED, False, False, True),
    ],
)
def test_handle_cancel_job(db, initial_state, terminate, finalize, cleanup):
    api = StubExecutorAPI()

    task, job_id = api.add_test_canceljob_task(initial_state)

    main.handle_single_task(task, api)

    task = controller_task_api.get_task(task.id)
    # All tasks end up in FINALIZED, even if they've errored or haven't
    # started yet
    assert task.agent_stage == ExecutorState.FINALIZED.value
    assert task.agent_complete

    assert (job_id in api.tracker["terminate"]) == terminate
    assert (job_id in api.tracker["finalize"]) == finalize
    assert (job_id in api.tracker["cleanup"]) == cleanup

    spans = get_trace("agent_loop")
    assert len(spans) == 1
    span = spans[0]
    assert span.attributes["initial_job_status"] == initial_state.name
    assert span.attributes["final_job_status"] == ExecutorState.FINALIZED.name
