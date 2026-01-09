import pytest

from common.job_executor import JobDefinition
from controller import task_api
from controller.main import job_to_job_definition
from controller.models import Task, TaskType
from tests.factories import job_factory


def test_insert_runjob_task(db):
    job = job_factory()
    task_id = job.id
    task = Task(
        id=task_id,
        backend="test",
        type=TaskType.RUNJOB,
        definition=job_to_job_definition(job, task_id).to_dict(),
    )

    task_api.insert_task(task)

    task = task_api.get_task(job.id)
    assert task.active
    job_definition = JobDefinition.from_dict(task.definition)
    assert job_definition == job_to_job_definition(job, task_id)


def test_mark_inactive(db):
    job = job_factory()
    task_id = job.id
    task = Task(
        id=task_id,
        backend="test",
        type=TaskType.RUNJOB,
        definition=job_to_job_definition(job, task_id).to_dict(),
    )

    task_api.insert_task(task)
    task = task_api.get_task(job.id)
    assert bool(task.active) is True

    task_api.mark_task_inactive(task)
    task = task_api.get_task(job.id)
    assert bool(task.active) is False


@pytest.mark.parametrize("task_type", list(TaskType))
@pytest.mark.parametrize("complete", [True, False])
def test_handle_task_update(db, task_type, complete):
    task = Task(
        id="task1",
        backend="test",
        type=task_type,
        definition={"some_key": "some_value"},
    )
    task_api.insert_task(task)

    task_api.handle_task_update(
        task_id="task1",
        stage="stage1",
        results={"some_result": "something"},
        complete=complete,
    )

    updated_task = task_api.get_task("task1")
    assert updated_task.active == (not complete)
    assert updated_task.agent_stage == "stage1"
    assert updated_task.agent_results == {"some_result": "something"}
    if complete:
        assert updated_task.finished_at > 0
    else:
        assert updated_task.finished_at is None
