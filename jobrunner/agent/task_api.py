
from jobrunner.controller.task_api import handle_task_update
from jobrunner.lib import database  # cheating!
from jobrunner.models import Task as ControllerTask  # cheating!
from jobrunner.schema import AgentTask


def get_active_tasks(backend: str) -> list[AgentTask]:
    """Get a list of active tasks for this backend from the controller"""
    # cheating for now - should be HTTP API call
    return [
        AgentTask.from_task(t)
        for t in database.find_where(ControllerTask, active=True, backend=backend)
    ]


def update_controller(
    task: AgentTask,
    stage: str,
    results: dict = None,
    complete: bool = False,
):
    """Update the controller with the current state of the task.

    stage: the current stage of the task from the agent's perspective
    results: optional dictionary of completed results of this task, expected to be immutable
    complete: if the agent considers this task complete
    """
    # Cheating! This will eventaully be an HTTP API call to the controller but we just
    # do a direct function call for now
    handle_task_update(task_id=task.id, stage=stage, results=results, complete=complete)
