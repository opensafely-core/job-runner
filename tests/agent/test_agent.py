from unittest.mock import Mock, patch

import pytest

from jobrunner.agent import main
from jobrunner.controller import task_api as controller_task_api
from jobrunner.job_executor import ExecutorState
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

    def finalize(job_id):
        freezer.tick(1)
        api.set_job_result(job_id)

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
