import time

import pytest
import requests
from responses import matchers

from agent import config, task_api
from common.schema import AgentTask
from controller import task_api as controller_api
from controller.lib.database import transaction
from tests.factories import runjob_db_task_factory


@pytest.fixture(autouse=True)
def setup_config(monkeypatch):
    monkeypatch.setattr(
        "controller.config.DEFAULT_JOB_CPU_COUNT",
        {
            "test": 2,
            "dummy": 2,
            "another": 2,
        },
    )
    monkeypatch.setattr(
        "controller.config.DEFAULT_JOB_MEMORY_LIMIT",
        {"test": "4G", "dummy": "4G", "another": "4G"},
    )


def test_get_active_tasks(db, monkeypatch, responses):
    monkeypatch.setattr("agent.config.BACKEND", "dummy")

    task1 = runjob_db_task_factory(backend="dummy")
    task2 = runjob_db_task_factory(backend="dummy")
    task3 = runjob_db_task_factory(backend="another")

    with transaction():
        controller_api.mark_task_inactive(task2)

    responses.add(
        method="GET",
        url=f"{config.TASK_API_ENDPOINT}dummy/tasks/",
        status=200,
        json={"tasks": [AgentTask.from_task(task1).asdict()]},
        match=[matchers.header_matcher({"Authorization": config.TASK_API_TOKEN})],
    )
    responses.add(
        method="GET",
        url=f"{config.TASK_API_ENDPOINT}another/tasks/",
        status=200,
        json={"tasks": [AgentTask.from_task(task3).asdict()]},
        match=[matchers.header_matcher({"Authorization": config.TASK_API_TOKEN})],
    )

    active = task_api.get_active_tasks()
    assert len(active) == 1
    assert active[0].id == task1.id

    monkeypatch.setattr("agent.config.BACKEND", "another")

    active = task_api.get_active_tasks()
    assert len(active) == 1
    assert active[0].id == task3.id


def test_get_active_tasks_api_error(db, monkeypatch, responses):
    monkeypatch.setattr("agent.config.BACKEND", "dummy")

    runjob_db_task_factory(backend="dummy")

    responses.add(
        method="GET",
        url=f"{config.TASK_API_ENDPOINT}dummy/tasks/",
        status=500,
        match=[matchers.header_matcher({"Authorization": config.TASK_API_TOKEN})],
    )

    with pytest.raises(requests.HTTPError):
        task_api.get_active_tasks()


def test_update_controller(db, monkeypatch, responses, live_server):
    monkeypatch.setattr("agent.config.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    task = runjob_db_task_factory(backend="test")

    task_api.update_controller(
        task,
        stage="FINALIZED",
        results={"test": "test"},
        complete=True,
    )

    db_task = controller_api.get_task(task.id)
    assert db_task.agent_stage == "FINALIZED"
    assert db_task.agent_results == {"test": "test"}
    assert bool(db_task.agent_complete) is True
    assert db_task.agent_timestamp_ns is None


def test_update_controller_with_timestamp(db, monkeypatch, responses, live_server):
    monkeypatch.setattr("agent.config.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    task = runjob_db_task_factory(backend="dummy")

    timestamp = time.time_ns()
    task_api.update_controller(
        task,
        stage="FINALIZED",
        results={"test": "test"},
        complete=True,
        timestamp_ns=timestamp,
    )

    db_task = controller_api.get_task(task.id)
    assert db_task.agent_stage == "FINALIZED"
    assert db_task.agent_results == {"test": "test"}
    assert bool(db_task.agent_complete) is True
    assert db_task.agent_timestamp_ns == timestamp


def test_full_job_stages(db, responses, monkeypatch, live_server):
    monkeypatch.setattr("agent.config.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)
    task = runjob_db_task_factory(backend="dummy")

    stages = [
        "PREPARING",
        "PREPARED",
        "EXECUTING",
        "EXECUTED",
    ]

    for stage in stages:
        task_api.update_controller(task, stage)
        db_task = controller_api.get_task(task.id)
        assert db_task.agent_stage == stage
        assert bool(db_task.agent_complete) is False

    task_api.update_controller(
        task, "FINALIZED", results={"test": "test"}, complete=True
    )
    db_task = controller_api.get_task(task.id)
    assert db_task.agent_stage == "FINALIZED"
    assert db_task.agent_results == {"test": "test"}
    assert bool(db_task.agent_complete) is True
