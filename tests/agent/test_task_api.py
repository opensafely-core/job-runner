from jobrunner.agent import task_api
from jobrunner.controller import task_api as controller_api
from tests.factories import runjob_db_task_factory


def test_get_active_jobs(db):
    task1 = runjob_db_task_factory()
    task2 = runjob_db_task_factory()
    controller_api.mark_task_inactive(task2)

    active = task_api.get_active_tasks()
    assert len(active) == 1
    assert active[0].id == task1.id


def test_update_controller(db):
    task = runjob_db_task_factory()

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


def test_full_job_stages(db):
    task = runjob_db_task_factory()

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
