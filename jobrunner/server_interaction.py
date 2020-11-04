import datetime
import logging
import os

import requests

from jobrunner import utils
from jobrunner.exceptions import DependencyFailed, DependencyRunning

logger = utils.getlogger(__name__)


def get_latest_matching_job_from_queue(workspace_id=None, action_id=None, **kw):
    job = {
        "backend": os.environ["BACKEND"],
        "workspace_id": workspace_id,
        "action_id": action_id,
        "limit": 1,
    }
    if kw["needed_by_id"] and kw["force_run"]:
        # When forcing a run, we don't want to consider previous successes or
        # failures related to other triggering actions.
        job["needed_by_id"] = kw["needed_by_id"]
    response = requests.get(
        os.environ["JOB_SERVER_ENDPOINT"], params=job, auth=utils.get_auth()
    )
    response.raise_for_status()
    results = response.json()["results"]
    return results[0] if results else None


def push_dependency_job_from_action_to_queue(action):
    job = utils.writable_job_subset(action)
    #
    response = requests.post(
        os.environ["JOB_SERVER_ENDPOINT"], json=job, auth=utils.get_auth()
    )
    response.raise_for_status()
    return response.json()


def mark_dependency_job_as_failed(action):
    job_data = utils.writable_job_subset(action)
    del job_data["workspace_id"]  # patching this is disallowed by the API
    job_data["status_code"] = -2
    job_data["status_message"] = "Docker never started"
    response = requests.patch(
        os.environ["JOB_SERVER_ENDPOINT"] + str(action["pk"]) + "/",
        json=job_data,
        auth=utils.get_auth(),
    )
    response.raise_for_status()
    return response.json()


def start_dependent_job_or_raise_if_unfinished(dependency_action):
    """Do the target output files for this job exist?  If not, raise an
    exception to prevent the dependent job from starting.

    `DependencyRunning` exceptions have special handling in the main
    loop so the dependent job can be retried as necessary

    """
    joblogger = logging.LoggerAdapter(
        logger, {"job_id": f"job#{dependency_action['needed_by_id']}"}
    )
    joblogger.debug(
        "Deciding if dependency action %s needs to be run: %s",
        dependency_action["action_id"],
        utils.writable_job_subset(dependency_action),
    )
    if not utils.needs_run(dependency_action):
        dependency_action["needs_run"] = False
        joblogger.debug(
            "Action %s does not need to be run, found files at %s",
            dependency_action["action_id"],
            utils.needs_run(dependency_action),
        )
        return
    else:
        joblogger.debug(
            "Action %s should be run if possible", dependency_action["action_id"],
        )

    if utils.docker_container_exists(dependency_action["container_name"]):
        raise DependencyRunning(
            f"Not started because dependency `{dependency_action['action_id']}` is currently running",
            report_args=True,
        )
    else:
        joblogger.debug(
            "Action %s is not currently running; checking previous run state",
            dependency_action["action_id"],
        )
    dependency_status = get_latest_matching_job_from_queue(**dependency_action)
    if not dependency_status:
        joblogger.debug(
            "No previous job found on queue: %s", dependency_action["action_id"],
        )
    else:
        joblogger.debug(
            "Got previous action %s (job#%s) from queue: %s",
            dependency_status["action_id"],
            dependency_status["pk"],
            dependency_status,
        )
        if dependency_status["status_code"] == DependencyRunning.status_code:
            raise DependencyRunning(
                f"Not started because dependency `{dependency_action['action_id']}` is currently running",
                report_args=True,
            )
        if dependency_status["completed_at"]:

            if dependency_status["force_run"]:
                dependency_action["needs_run"] = False
                joblogger.debug(
                    "Completed action %s was a `force_run` dependency; don't do it again",
                    dependency_action["action_id"],
                )
                return
            elif dependency_status["status_code"] == 0:
                joblogger.debug(
                    "Previous run of action %s succeeded",
                    dependency_action["action_id"],
                )
                new_job = push_dependency_job_from_action_to_queue(dependency_action)
                raise DependencyRunning(
                    f"Not started because dependency `{dependency_action['action_id']}` has been added to the job queue as job#{new_job['pk']} because its previous output can no longer be found",
                    report_args=True,
                )
            else:
                joblogger.debug(
                    "Previous run of action %s failed", dependency_action["action_id"],
                )
                raise DependencyFailed(
                    f"Dependency `{dependency_action['action_id']}` failed, so unable to run this action",
                    report_args=True,
                )

        elif dependency_status["started"]:
            # This branch exists to handle a state that can only occur if the
            # server has been killed, or similar
            joblogger.debug(
                "Previous run of action %s started but didn't complete",
                dependency_action["action_id"],
            )

            started_at = datetime.datetime.fromisoformat(
                dependency_status["started_at"].replace("Z", "")
            )
            elapsed = datetime.datetime.now() - started_at
            if elapsed.seconds > 60 * 60 * 24:
                joblogger.debug(
                    "Previous run of action %s never started; cancelling",
                    dependency_action["action_id"],
                )
                mark_dependency_job_as_failed(dependency_status)
                raise DependencyFailed(
                    f"Dependency `{dependency_action['action_id']}` failed"
                )
            raise DependencyRunning(
                f"Not started because dependency `{dependency_action['action_id']}` is just about to start",
                report_args=True,
            )
        else:
            raise DependencyRunning(
                f"Not started because dependency `{dependency_action['action_id']}` is waiting to start",
                report_args=True,
            )

    new_job = push_dependency_job_from_action_to_queue(dependency_action)
    joblogger.debug(
        "Pushed new job to queue: %s", utils.writable_job_subset(new_job),
    )
    raise DependencyRunning(
        f"Not started because dependency `{dependency_action['action_id']}` has been added to the job queue",
        report_args=True,
    )
