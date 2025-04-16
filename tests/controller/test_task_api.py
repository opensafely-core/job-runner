from jobrunner.controller import task_api
from jobrunner.controller.main import job_to_job_definition
from jobrunner.job_executor import JobDefinition
from jobrunner.models import Task, TaskType
from tests.factories import job_factory


def test_insert_runjob_task(db):
    job = job_factory()

    task = Task(
        id=job.id,
        backend="test",
        type=TaskType.RUNJOB,
        definition=job_to_job_definition(job).to_dict(),
    )

    task_api.insert_task(task)

    task = task_api.get_task(job.id)
    assert task.active
    job_definition = JobDefinition.from_dict(task.definition)
    assert job_definition == job_to_job_definition(job)


def test_mark_inactive(db):
    job = job_factory()

    task = Task(
        id=job.id,
        backend="test",
        type=TaskType.RUNJOB,
        definition=job_to_job_definition(job).to_dict(),
    )

    task_api.insert_task(task)
    task = task_api.get_task(job.id)
    assert bool(task.active) is True

    task_api.mark_task_inactive(task)
    task = task_api.get_task(job.id)
    assert bool(task.active) is False
