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

    task, job_id = api.add_test_task(ExecutorState.UNKNOWN)

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

    task, job_id = api.add_test_task(ExecutorState.UNKNOWN)

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
