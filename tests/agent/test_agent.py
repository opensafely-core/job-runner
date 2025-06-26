import logging
import sqlite3
import time
from unittest.mock import Mock, patch

import pytest

from jobrunner.agent import main, task_api
from jobrunner.config import agent as config
from jobrunner.controller import task_api as controller_task_api
from jobrunner.job_executor import ExecutorState, JobDefinition
from jobrunner.lib.database import update_where
from jobrunner.models import Task, TaskType
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


def test_handle_tasks_error(db, caplog, responses, live_server, monkeypatch):
    monkeypatch.setattr("jobrunner.config.agent.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.UNKNOWN)
    api.set_job_transition(
        job_id, ExecutorState.PREPARED, hook=Mock(side_effect=Exception("task error"))
    )

    msg = "Some tasks failed, restarting agent loop"
    with pytest.raises(Exception, match=msg):
        main.handle_tasks(api)

    spans = get_trace("agent_loop")

    assert spans[0].name == "LOOP_TASK"
    assert spans[1].name == "AGENT_LOOP"
    assert spans[1].attributes["handled_tasks"] == 1
    assert spans[1].attributes["errored_tasks"] == 1

    assert caplog.records[0].msg == "task error"


def test_handle_job_full_execution(
    db, freezer, caplog, responses, live_server, monkeypatch
):
    monkeypatch.setattr("jobrunner.config.agent.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    caplog.set_level(logging.INFO)
    # move to a whole second boundary for easier timestamp maths
    freezer.move_to("2022-01-01T12:34:56")

    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.UNKNOWN)

    freezer.tick(1)

    # prepare is synchronous
    prepared_timestamp_ns = time.time_ns()
    api.set_job_transition(
        job_id,
        ExecutorState.PREPARED,
        prepared_timestamp_ns,
        hook=lambda j: freezer.tick(1),
    )

    main.handle_single_task(task, api)

    task = controller_task_api.get_task(task.id)
    assert task.agent_stage == ExecutorState.PREPARED.value
    assert task.agent_timestamp_ns == prepared_timestamp_ns

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

    finalized_timestamp_ns = prepared_timestamp_ns + 1000
    api.set_job_transition(
        job_id, ExecutorState.FINALIZED, finalized_timestamp_ns, hook=finalize
    )
    assert job_id not in api.tracker["finalize"]
    main.handle_single_task(task, api)
    assert job_id in api.tracker["finalize"]
    task = controller_task_api.get_task(task.id)
    assert task.agent_stage == ExecutorState.FINALIZED.value
    assert task.agent_complete
    assert task.agent_results
    assert task.agent_timestamp_ns == finalized_timestamp_ns

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

    assert all(s.attributes["backend"] == "test" for s in spans)

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
    timestamp_ns = time.time_ns()
    task, job_id = api.add_test_runjob_task(executor_state, timestamp_ns=timestamp_ns)
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
        timestamp_ns,
    )

    assert job.id not in api.tracker["prepare"]
    assert job.id not in api.tracker["execute"]
    assert job.id not in api.tracker["finalize"]

    # no spans
    assert len(get_trace("jobs")) == 0
    spans = get_trace("agent_loop")
    assert all(s.attributes["backend"] == "test" for s in spans)
    assert len(spans) == 1

    assert spans[0].attributes["initial_job_status"] == executor_state.name
    assert spans[0].attributes["final_job_status"] == executor_state.name


@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_handle_job_requires_db_has_secrets(mock_update_controller, db, monkeypatch):
    api = StubExecutorAPI()
    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    monkeypatch.setattr(config, "DATABASE_URLS", {None: "dburl"})

    task, job_id = api.add_test_runjob_task(ExecutorState.PREPARED, requires_db=True)

    def check_env(definition):
        assert definition.env["DATABASE_URL"] == "dburl"

    api.set_job_transition(job_id, ExecutorState.EXECUTING, hook=check_env)

    main.handle_run_job_task(task, api)

    assert mock_update_controller.call_count == 1


@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_handle_runjob_with_fatal_error(mock_update_controller, db):
    api = StubExecutorAPI()

    preprared_timestamp_ns = time.time_ns()
    task, job_id = api.add_test_runjob_task(
        ExecutorState.PREPARED, timestamp_ns=preprared_timestamp_ns
    )

    executing_timestamp_ns = preprared_timestamp_ns + 10
    api.set_job_transition(
        job_id,
        ExecutorState.EXECUTING,
        timestamp_ns=executing_timestamp_ns,
        hook=Mock(side_effect=Exception("test_hard_failure")),
    )
    with pytest.raises(Exception, match="test_hard_failure"):
        main.handle_single_task(task, api)

    assert mock_update_controller.call_count == 1
    call_kwargs = mock_update_controller.call_args[1]
    assert call_kwargs["task"] == task
    assert call_kwargs["stage"] == ExecutorState.ERROR.value
    results = call_kwargs["results"]
    assert results["error"]["exception"] == "Exception"
    assert results["error"]["message"] == "test_hard_failure"
    assert "traceback" in results["error"]
    assert call_kwargs["complete"] is True
    # The fatal error triggers a call to finalize, which sets the timestamp_ns
    # to the current time_ns
    assert call_kwargs["timestamp_ns"] > executing_timestamp_ns

    spans = get_trace("agent_loop")
    assert all(s.attributes["backend"] == "test" for s in spans)
    assert len(spans) == 1
    span = spans[0]
    assert len(span.events) == 1
    assert "test_hard_failure" in span.events[0].attributes["exception.message"]

    assert span.attributes["initial_job_status"] == "PREPARED"
    assert span.attributes["final_job_status"] == "ERROR"
    # exception info has been added to the span
    assert span.status.status_code.name == "ERROR"
    assert span.status.description == "Exception: test_hard_failure"
    assert spans[0].attributes["fatal_task_error"] is True


@pytest.mark.parametrize(
    "exc",
    [
        sqlite3.OperationalError("database locked"),
        AssertionError("a bad thing"),
    ],
)
@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_handle_runjob_with_not_fatal_error(mock_update_controller, db, exc):
    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.PREPARED)

    api.set_job_transition(
        job_id,
        ExecutorState.EXECUTING,
        hook=Mock(side_effect=exc),
    )

    with pytest.raises(Exception, match=str(exc)):
        main.handle_single_task(task, api)

    # Controller is not notified of transient error
    assert mock_update_controller.call_count == 0

    spans = get_trace("agent_loop")
    assert spans[0].status.status_code.name == "ERROR"
    assert spans[0].status.description == f"{exc.__class__.__name__}: {str(exc)}"
    assert spans[0].attributes["fatal_task_error"] is False


@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_handle_canceljob_with_fatal_error(mock_update_controller, db):
    api = StubExecutorAPI()

    executed_timestamp_ns = time.time_ns()
    task, job_id = api.add_test_canceljob_task(
        ExecutorState.EXECUTED, executed_timestamp_ns
    )

    finalized_timestamp_ns = executed_timestamp_ns + 10
    api.set_job_transition(
        job_id,
        ExecutorState.FINALIZED,
        timestamp_ns=finalized_timestamp_ns,
        hook=Mock(side_effect=Exception("test_hard_failure")),
    )

    with pytest.raises(Exception):
        main.handle_single_task(task, api)

    # canceljob handler always calls update first
    assert mock_update_controller.call_count == 2
    call_kwargs = mock_update_controller.call_args[1]
    assert call_kwargs["task"] == task
    assert call_kwargs["stage"] == ExecutorState.ERROR.value
    results = call_kwargs["results"]
    assert results["error"]["exception"] == "Exception"
    assert results["error"]["message"] == "test_hard_failure"
    assert "traceback" in results["error"]
    assert call_kwargs["complete"] is True
    # The fatal error triggers a call to finalize, which sets the timestamp_ns
    # to the current time_ns
    assert call_kwargs["timestamp_ns"] > executed_timestamp_ns

    spans = get_trace("agent_loop")
    assert len(spans) == 1
    span = spans[0]
    assert span.attributes["initial_job_status"] == "EXECUTED"
    assert span.attributes["final_job_status"] == "ERROR"
    # exception info has been added to the span
    assert span.status.status_code.name == "ERROR"
    assert span.status.description == "Exception: test_hard_failure"
    assert spans[0].attributes["fatal_task_error"] is True


@pytest.mark.parametrize(
    "initial_state,interim_state,terminate,finalize",
    [
        (ExecutorState.PREPARED, None, False, True),
        (ExecutorState.EXECUTING, ExecutorState.EXECUTED, True, True),
        (ExecutorState.UNKNOWN, None, False, True),
        (ExecutorState.EXECUTED, None, False, True),
        (ExecutorState.ERROR, None, False, True),
        (ExecutorState.FINALIZED, None, False, False),
    ],
)
def test_handle_cancel_job(
    db,
    caplog,
    initial_state,
    interim_state,
    terminate,
    finalize,
    monkeypatch,
    responses,
    live_server,
):
    monkeypatch.setattr("jobrunner.config.agent.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

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
    # clean up is run irrespective of initial state
    assert job_id in api.tracker["cleanup"]

    spans = get_trace("agent_loop")
    assert len(spans) == 1
    span = spans[0]
    assert span.attributes["initial_job_status"] == initial_state.name
    assert span.attributes["final_job_status"] == ExecutorState.FINALIZED.name


@patch("jobrunner.agent.main.docker", autospec=True)
@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_handle_db_status_job(mock_update_controller, mock_docker, monkeypatch):
    monkeypatch.setattr(
        config, "DATABASE_URLS", {"default": "database://localhost:1234"}
    )
    mock_docker.return_value = Mock(stdout="line 1\nline 2\ndb-maintenance ")

    task = Task(
        id="test_id",
        backend="test",
        type=TaskType.DBSTATUS,
        definition={"database_name": "default"},
        created_at=time.time(),
    )

    main.handle_single_task(task, api=None)

    mock_docker.assert_called_with(
        [
            "run",
            "--rm",
            "-e",
            "DATABASE_URL",
            "--network",
            "jobrunner-db",
            "--dns",
            "192.0.2.0",
            "--add-host",
            "localhost:127.0.0.1",
            "ghcr.io/opensafely-core/tpp-database-utils",
            "in_maintenance_mode",
        ],
        env={"DATABASE_URL": "database://localhost:1234"},
        check=True,
        capture_output=True,
        text=True,
    )

    mock_update_controller.assert_called_with(
        task,
        stage="",
        results={"results": {"status": "db-maintenance"}, "error": None},
        complete=True,
    )


@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_handle_db_status_job_with_error(mock_update_controller):
    task = Task(
        id="test_id",
        backend="test",
        type=TaskType.DBSTATUS,
        definition={"database_name": "no_such_database"},
        created_at=time.time(),
    )

    main.handle_single_task(task, api=None)

    mock_update_controller.assert_called_with(
        task,
        stage="",
        results={
            "results": None,
            "error": {"type": "KeyError", "message": "'no_such_database'"},
        },
        complete=True,
    )


@patch("jobrunner.agent.main.docker", autospec=True)
def test_db_status_task_rejects_unexpected_status(mock_docker, monkeypatch):
    monkeypatch.setattr(
        config, "DATABASE_URLS", {"default": "database://localhost:1234"}
    )
    mock_docker.return_value = Mock(stdout="unexpected value")
    with pytest.raises(ValueError, match="Invalid status") as exc:
        main.db_status_task(database_name="default")
    assert "unexpected value" not in str(exc.value)


def test_handle_job_no_task_id_in_definition(
    db, freezer, caplog, responses, live_server, monkeypatch
):
    monkeypatch.setattr("jobrunner.config.agent.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    caplog.set_level(logging.INFO)
    # move to a whole second boundary for easier timestamp maths
    freezer.move_to("2022-01-01T12:34:56")

    api = StubExecutorAPI()

    task, _ = api.add_test_runjob_task(ExecutorState.UNKNOWN)

    task.definition.pop("task_id")
    update_where(Task, {"definition": task.definition}, id=task.id)

    main.handle_single_task(task, api)

    task = controller_task_api.get_task(task.id)
    assert task.agent_stage == ExecutorState.PREPARED.value
    assert "task_id" not in task.definition
