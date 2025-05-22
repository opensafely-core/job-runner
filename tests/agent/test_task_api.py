import time

import pytest

from jobrunner.agent import task_api
from jobrunner.controller import task_api as controller_api
from tests.factories import runjob_db_task_factory


@pytest.fixture(autouse=True)
def setup_config(monkeypatch):
    monkeypatch.setattr(
        "jobrunner.config.controller.DEFAULT_JOB_CPU_COUNT",
        {
            "dummy": 2,
            "another": 2,
        },
    )
    monkeypatch.setattr(
        "jobrunner.config.controller.DEFAULT_JOB_MEMORY_LIMIT",
        {"dummy": "4G", "another": "4G"},
    )


def test_get_active_jobs(db):
    task1 = runjob_db_task_factory(backend="dummy")
    task2 = runjob_db_task_factory(backend="dummy")
    task3 = runjob_db_task_factory(backend="another")
    controller_api.mark_task_inactive(task2)

    active = task_api.get_active_tasks(backend="dummy")
    assert len(active) == 1
    assert active[0].id == task1.id

    active = task_api.get_active_tasks(backend="another")
    assert len(active) == 1
    assert active[0].id == task3.id


def test_update_controller(db):
    task = runjob_db_task_factory(backend="dummy")

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


def test_update_controller_with_timestamp(db):
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


def test_full_job_stages(db):
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
