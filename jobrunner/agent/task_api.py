import logging
from urllib.parse import urljoin

import requests

from jobrunner.config import agent as config
from jobrunner.controller import task_api as controller_task_api  # cheating!
from jobrunner.schema import AgentTask


log = logging.getLogger(__name__)


class TaskApi:
    def __init__(self):
        self.base_url = urljoin(config.TASK_API_ENDPOINT, config.BACKEND)
        self.session = requests.Session()

    def get_json(self, path):
        return self.request_json("GET", path)

    def request_json(self, method, path, data=None):
        data = data or {}
        url = f"{self.base_url}/{path}"
        response = self.session.request(method, url, data=data)
        try:
            response.raise_for_status()
        except Exception as e:
            log.exception(e)
            raise
        return response.json()


def get_active_tasks(backend: str) -> list[AgentTask]:
    """Get a list of active tasks for this backend from the controller"""
    # cheating for now - should be HTTP API call
    return [
        AgentTask.from_task(t) for t in controller_task_api.get_active_tasks(backend)
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
    controller_task_api.handle_task_update(
        task_id=task.id, stage=stage, results=results, complete=complete
    )
