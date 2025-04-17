import logging
from unittest.mock import Mock, patch

import pytest

from jobrunner import config
from jobrunner.agent import main, task_api
from jobrunner.controller import task_api as controller_task_api
from jobrunner.job_executor import ExecutorState, JobDefinition
from tests.agent.stubs import StubExecutorAPI
from tests.conftest import get_trace


def assert_state_change_logs(caplog, state_changes):
    state_change_logs = [
        record
        for record in caplog.records
        if record.levelno == logging.INFO
        if "State change" in record.msg
    ]
    for i, (from_state, to_state) in enumerate(state_changes):
        assert f"{from_state} -> {to_state}" in state_change_logs[i].msg, [
            rec.msg for rec in state_change_logs
        ]


def test_handle_job_full_execution(db, freezer, caplog):
    caplog.set_level(logging.INFO)
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

    # expected status transitions so far
    state_changes = [
        (ExecutorState.UNKNOWN, ExecutorState.PREPARING),
        (ExecutorState.PREPARING, ExecutorState.PREPARED),
    ]
    assert_state_change_logs(caplog, state_changes)

    freezer.tick(1)
    main.handle_single_task(task, api)
    task = controller_task_api.get_task(task.id)
    assert task.agent_stage == ExecutorState.EXECUTING.value

    state_changes.append((ExecutorState.PREPARED, ExecutorState.EXECUTING))
    assert_state_change_logs(caplog, state_changes)

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
    assert task.agent_results

    # Note EXECUTING -> EXECUTED happens outside of the agent loop
    # handler, so in this last call to handle_single_task, the job
    # started in EXECUTED state
    state_changes.extend(
        [
            (ExecutorState.EXECUTED, ExecutorState.FINALIZING),
            (ExecutorState.FINALIZING, ExecutorState.FINALIZED),
        ]
    )
    assert_state_change_logs(caplog, state_changes)

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
        {},
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


@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_handle_runjob_with_error(mock_update_controller, db):
    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.PREPARED)

    api.set_job_transition(
        job_id, ExecutorState.EXECUTING, hook=Mock(side_effect=Exception("foo"))
    )

    with pytest.raises(Exception):
        main.handle_single_task(task, api)

    assert mock_update_controller.call_count == 1
    task, stage, results, complete = mock_update_controller.call_args[0]
    assert task == task
    assert stage == ExecutorState.ERROR.value
    assert results["error"]["exception"] == "Exception"
    assert results["error"]["message"] == "foo"
    assert "traceback" in results["error"]
    assert complete == complete

    spans = get_trace("agent_loop")
    assert len(spans) == 1
    span = spans[0]
    assert span.attributes["initial_job_status"] == "PREPARED"
    assert span.attributes["final_job_status"] == "ERROR"
    # exception info has been added to the span
    assert span.status.status_code.name == "ERROR"
    assert span.status.description == "Exception: foo"


@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_handle_canceljob_with_error(mock_update_controller, db):
    api = StubExecutorAPI()

    task, job_id = api.add_test_canceljob_task(ExecutorState.EXECUTED)

    api.set_job_transition(
        job_id, ExecutorState.FINALIZED, hook=Mock(side_effect=Exception("foo"))
    )

    with pytest.raises(Exception):
        main.handle_single_task(task, api)

    # canceljob handler always calls update first
    assert mock_update_controller.call_count == 2
    task, stage, results, complete = mock_update_controller.call_args[0]
    assert task == task
    assert stage == ExecutorState.ERROR.value
    assert results["error"]["exception"] == "Exception"
    assert results["error"]["message"] == "foo"
    assert "traceback" in results["error"]
    assert complete == complete

    spans = get_trace("agent_loop")
    assert len(spans) == 1
    span = spans[0]
    assert span.attributes["initial_job_status"] == "EXECUTED"
    assert span.attributes["final_job_status"] == "ERROR"
    # exception info has been added to the span
    assert span.status.status_code.name == "ERROR"
    assert span.status.description == "Exception: foo"


@pytest.mark.parametrize(
    "initial_state,interim_state,terminate,finalize,cleanup",
    [
        (ExecutorState.PREPARED, None, False, True, False),
        (ExecutorState.EXECUTING, ExecutorState.EXECUTED, True, True, True),
        (ExecutorState.UNKNOWN, None, False, True, False),
        (ExecutorState.EXECUTED, None, False, True, True),
        (ExecutorState.ERROR, None, False, True, True),
        (ExecutorState.FINALIZED, None, False, False, True),
    ],
)
def test_handle_cancel_job(
    db, caplog, initial_state, interim_state, terminate, finalize, cleanup
):
    caplog.set_level(logging.INFO)

    api = StubExecutorAPI()

    task, job_id = api.add_test_canceljob_task(initial_state)

    main.handle_single_task(task, api)

    # expected status transitions
    state_changes = [
        (None, initial_state),
    ]
    if interim_state:
        state_changes.append((initial_state, interim_state))
        state_changes.append((interim_state, ExecutorState.FINALIZED))
    elif initial_state != ExecutorState.FINALIZED:
        state_changes.append((initial_state, ExecutorState.FINALIZED))
    assert_state_change_logs(caplog, state_changes)

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
