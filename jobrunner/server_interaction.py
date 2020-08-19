import logging
import os

import requests

from jobrunner.exceptions import DependencyFailed, DependencyRunning
from jobrunner.utils import docker_container_exists, get_auth, getlogger, needs_run

logger = getlogger(__name__)
baselogger = logging.LoggerAdapter(logger, {"job_id": "-"})


def get_latest_matching_job_from_queue(workspace=None, action_id=None, **kw):
    job = {
        "backend": os.environ["BACKEND"],
        "workspace_id": workspace["id"],
        "operation": action_id,
        "limit": 1,
    }
    response = requests.get(
        os.environ["JOB_SERVER_ENDPOINT"], params=job, auth=get_auth()
    )
    response.raise_for_status()
    results = response.json()["results"]
    return results[0] if results else None


def push_dependency_job_from_action_to_queue(action):
    job = {
        "backend": os.environ["BACKEND"],
        "workspace_id": action["workspace"]["id"],
        "operation": action["action_id"],
    }
    job["callback_url"] = action["callback_url"]
    job["needed_by"] = action["needed_by"]
    response = requests.post(
        os.environ["JOB_SERVER_ENDPOINT"], json=job, auth=get_auth()
    )
    response.raise_for_status()
    return response


def start_dependent_job_or_raise_if_unfinished(dependency_action):
    """Do the target output files for this job exist?  If not, raise an
    exception to prevent the dependent job from starting.

    `DependencyRunning` exceptions have special handling in the main
    loop so the dependent job can be retried as necessary

    """
    if not needs_run(dependency_action):
        # We ingore any existing `needs_run` key and recheck, because
        # this code path is run asynchronously, and things may have
        # changed since the project file was parsed.
        dependency_action["needs_run"] = False
        return
    dependency_action["needs_run"] = True
    if docker_container_exists(dependency_action["container_name"]):
        raise DependencyRunning(
            f"Not started because dependency `{dependency_action['action_id']}` is currently running (as {dependency_action['container_name']})",
            report_args=True,
        )

    dependency_status = get_latest_matching_job_from_queue(**dependency_action)
    baselogger.info(
        "Got job %s from queue to match %s",
        dependency_status,
        dependency_action["action_id"],
    )
    if dependency_status:
        if dependency_status["completed_at"]:
            if dependency_status["status_code"] == 0:
                new_job = push_dependency_job_from_action_to_queue(dependency_action)
                raise DependencyRunning(
                    f"Not started because dependency `{dependency_action['action_id']}` has been added to the job queue at {new_job['url']} as its previous output can no longer be found",
                    report_args=True,
                )
            else:
                raise DependencyFailed(
                    f"Dependency `{dependency_action['action_id']}` failed, so unable to run this operation",
                    report_args=True,
                )

        elif dependency_status["started"]:
            raise DependencyRunning(
                f"Not started because dependency `{dependency_action['action_id']}` is just about to start",
                report_args=True,
            )
        else:
            raise DependencyRunning(
                f"Not started because dependency `{dependency_action['action_id']}` is waiting to start",
                report_args=True,
            )

    push_dependency_job_from_action_to_queue(dependency_action)
    raise DependencyRunning(
        f"Not started because dependency `{dependency_action['action_id']}` has been added to the job queue",
        report_args=True,
    )
