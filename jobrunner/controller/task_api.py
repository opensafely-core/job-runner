import time

from jobrunner.lib import database
from jobrunner.models import Task


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
    task.completed_at = int(time.time())
    database.update(task)


def get_task(task_id):
    """Lookup a single task by id."""
    return database.find_one(Task, id=task_id)
