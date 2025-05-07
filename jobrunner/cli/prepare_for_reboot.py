"""
Ops utility for killing all running jobs and reseting them to PENDING so they will be
automatically re-run after a reboot.
"""

import argparse

from jobrunner.config import agent as agent_config
from jobrunner.controller.main import get_task_for_job, set_code
from jobrunner.controller.task_api import mark_task_inactive
from jobrunner.executors import volumes
from jobrunner.executors.local import container_name, docker
from jobrunner.lib.database import find_where
from jobrunner.models import Job, State, StatusCode


def main(pause=True):
    # TODO: pass this in as a cli arg when run from the controller
    backend = agent_config.BACKEND
    if pause:
        print(
            "== DANGER ZONE ==\n"
            "\n"
            f"This will kill all running jobs on backend '{backend}' and reset them to the PENDING state, ready\n"
            "to be restarted following a reboot.\n"
            "\n"
            "It should only be run when the job-runner service has been stopped."
            "\n"
        )
        confirm = input("Are you sure you want to continue? (y/N)")
        assert confirm.strip().lower() == "y"

    for job in find_where(Job, state=State.RUNNING, backend=backend):
        print(f"resetting job {job.slug} to PENDING")
        set_code(
            job,
            StatusCode.WAITING_ON_REBOOT,
            "Job restarted - waiting for server to reboot",
        )
        runjob_task = get_task_for_job(job)
        if runjob_task and runjob_task.active:
            print(f"setting task {runjob_task.id} to inactive")
            mark_task_inactive(runjob_task)
        # these are idempotent
        docker.kill(container_name(job))
        docker.delete_container(container_name(job))
        volumes.delete_volume(job)


def run():  # pragma: no cover
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    args = parser.parse_args()
    main(**vars(args))


if __name__ == "__main__":
    run()  # pragma: no cover
