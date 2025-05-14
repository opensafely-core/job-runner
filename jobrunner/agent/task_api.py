import json
import logging
from urllib.parse import urljoin

import requests

from jobrunner.config import agent as config
from jobrunner.schema import AgentTask


log = logging.getLogger(__name__)


class TaskApi:
    def __init__(self):
        self.base_url = urljoin(config.TASK_API_ENDPOINT, config.BACKEND)
        self.session = requests.Session()

    def get_json(self, path):
        return self.request_json("GET", path)

    def post_json(self, path, data=None):
        return self.request_json("POST", path, data)

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


def get_active_tasks() -> list[AgentTask]:
    """Get a list of active tasks for this backend from the controller"""
    api = TaskApi()
    agent_tasks = api.get_json("tasks")["tasks"]
    return [AgentTask.from_dict(t) for t in agent_tasks]


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

    post_data = {
        "task_id": task.id,
        "stage": stage,
        "results": results,
        "complete": complete,
    }

    api = TaskApi()
    api.post_json("task/update/", {"payload": json.dumps(post_data)})
