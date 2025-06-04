import json
import logging
from urllib.parse import urljoin

import requests

from jobrunner.config import agent as config
from jobrunner.schema import AgentTask


log = logging.getLogger(__name__)

session = requests.Session()


def get_json(path):
    return request_json("GET", path)


def post_json(path, data=None):
    return request_json("POST", path, data)


def request_json(method, path, data=None):
    base_url = urljoin(config.TASK_API_ENDPOINT, config.BACKEND)
    data = data or {}
    url = f"{base_url}/{path}"
    headers = {"Authorization": config.TASK_API_TOKEN}
    response = session.request(method, url, data=data, headers=headers)
    try:
        response.raise_for_status()
    except Exception as e:
        log.exception(e)
        raise
    return response.json()


def get_active_tasks() -> list[AgentTask]:
    """Get a list of active tasks for this backend from the controller"""
    agent_tasks = get_json("tasks/")["tasks"]
    return [AgentTask.from_dict(t) for t in agent_tasks]


def update_controller(
    task: AgentTask,
    stage: str,
    results: dict = None,
    complete: bool = False,
    timestamp_ns: int = None,
):
    """Update the controller with the current state of the task.

    stage: the current stage of the task from the agent's perspective
    results: optional dictionary of completed results of this task, expected to be immutable
    complete: if the agent considers this task complete
    timestamp_ns: Optional timestamp (in ns) of this state change. Can be None for tasks that
    do not involve state changes.

    Nb. If results contains an error key, the task is considered to have failed.
    """

    post_data = {
        "task_id": task.id,
        "stage": stage,
        "results": results,
        "complete": complete,
        "timestamp_ns": timestamp_ns,
    }

    post_json("task/update/", {"payload": json.dumps(post_data)})
