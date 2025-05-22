import time

from jobrunner.lib import database
from jobrunner.models import Task, TaskType
from jobrunner.queries import set_flag


def insert_task(task):
    """Insert a new task in task queue.

    Enures active is True and records creation time
    """
    task.created_at = int(time.time())
    task.active = True
    database.insert(task)


def mark_task_inactive(task):
    """Makes a test inactive

    This means the controller will no longer ask the agent for updates about
    it, and records the completion time.
    """
    task.active = False
    task.finished_at = int(time.time())
    database.update(task)


def get_task(task_id):
    """Lookup a single task by id."""
    return database.find_one(Task, id=task_id)


def get_active_tasks(backend: str) -> list[Task]:
    """Return list of active tasks to be sent to the agent for the supplied backend"""
    active_tasks = database.find_where(Task, active=True, backend=backend)
    # This is a small hack to ensure that the controller always receives the results of
    # DBSTATUS tasks before the results of RUNJOB tasks so that if the jobs have failed
    # because we've just entered database maintenance then the controller will handle
    # the failures correctly. This not the proper fix, but it's cheap to do, has little
    # downside and may help. See:
    # https://github.com/opensafely-core/job-runner/issues/893
    active_tasks.sort(key=lambda task: 0 if task.type == TaskType.DBSTATUS else 1)
    return active_tasks


def handle_task_update(*, task_id, stage, results, complete, timestamp_ns=None):
    # This is the function we expect to eventually be invoked via an HTTP API call.
    # This currently just updates the task table, and lets the main controller loop
    # update the jobs table as needed, and handle a completed task. In the future, we
    # may want the HTTP handler to do both, so that the main loop does not need to
    # handle agent updates and completed jobs at all. But all we have currently is the
    # loop, so we'll do that logic there for step 1.
    task = database.find_one(Task, id=task_id)
    task.agent_stage = stage
    task.agent_results = results
    task.agent_complete = complete
    if timestamp_ns:
        task.agent_timestamp_ns = int(timestamp_ns)
    if complete:
        task.active = False
        task.finished_at = int(time.time())

    match task.type:
        case TaskType.RUNJOB | TaskType.CANCELJOB:
            database.update(task)
        case TaskType.DBSTATUS:
            handle_task_update_dbstatus(task)
        case _:
            assert False, f"Unknown task type {task.type}"


def handle_task_update_dbstatus(task):
    with database.transaction():
        if results := task.agent_results.get("results"):
            mode = results["status"]
            set_flag("mode", mode, backend=task.backend)
        database.update(task)
