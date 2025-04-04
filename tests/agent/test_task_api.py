import time

from jobrunner.agent import task_api
from jobrunner.controller import task_api as controller_api
from jobrunner.schema import TaskStage
from tests.factories import runjob_task_factory


def test_get_active_jobs(db):
    task1 = runjob_task_factory()
    task2 = runjob_task_factory()
    controller_api.mark_task_inactive(task2)

    active = task_api.get_active_tasks()
    assert len(active) == 1
    assert active[0].id == task1.id


def test_update_controller(db):
    task = runjob_task_factory()

    timestamp_ns = int(time.time())
    task_api.update_controller(
        task,
        stage=TaskStage.FINALIZED,
        timestamp=timestamp_ns,
        results={"test": "test"},
        complete=True,
    )

    db_task = controller_api.get_task(task.id)
    assert db_task.agent_stage == TaskStage.FINALIZED
    assert db_task.agent_stage_ns == timestamp_ns
    assert db_task.agent_results == {"test": "test"}
    assert bool(db_task.agent_complete) is True


def test_full_job_stages(db):
    task = runjob_task_factory()
    timestamp_ns = int(time.time() * 1e9)

    stages = [
        TaskStage.PREPARING,
        TaskStage.PREPARED,
        TaskStage.EXECUTING,
        TaskStage.EXECUTED,
        TaskStage.FINALIZING,
    ]

    for stage in stages:
        timestamp_ns += int(1e6)  # 1ms
        task_api.update_controller(task, stage, timestamp_ns)
        db_task = controller_api.get_task(task.id)
        assert db_task.agent_stage == stage
        assert db_task.agent_stage_ns == timestamp_ns
        assert bool(db_task.agent_complete) is False

    timestamp_ns += int(1e6)
    task_api.update_controller(
        task, TaskStage.FINALIZED, timestamp_ns, results={"test": "test"}, complete=True
    )
    db_task = controller_api.get_task(task.id)
    assert db_task.agent_stage == TaskStage.FINALIZED
    assert db_task.agent_stage_ns == timestamp_ns
    assert db_task.agent_results == {"test": "test"}
    assert bool(db_task.agent_complete) is True
