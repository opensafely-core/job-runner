from common.job_executor import JobDefinition
from controller import task_api
from controller.lib.database import transaction
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

    with transaction():
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

    with transaction():
        task_api.insert_task(task)
    task = task_api.get_task(job.id)
    assert bool(task.active) is True

    with transaction():
        task_api.mark_task_inactive(task)
    task = task_api.get_task(job.id)
    assert bool(task.active) is False
