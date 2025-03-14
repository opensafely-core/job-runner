"""
Ops utility for killing all running jobs and reseting them to PENDING so they will be
automatically re-run after a reboot.
"""

import argparse

from jobrunner.executors.local import container_name, docker
from jobrunner.executors.volumes import get_volume_api
from jobrunner.lib.database import find_where
from jobrunner.models import Job, State, StatusCode
from jobrunner.run import set_code


def main(pause=True):
    if pause:
        print(
            "== DANGER ZONE ==\n"
            "\n"
            "This will kill all running jobs and reset them to the PENDING state, ready\n"
            "to be restarted following a reboot.\n"
            "\n"
            "It should only be run when the job-runner service has been stopped."
            "\n"
        )
        confirm = input("Are you sure you want to continue? (y/N)")
        assert confirm.strip().lower() == "y"

    for job in find_where(Job, state=State.RUNNING):
        print(f"reseting job {job.id} to PENDING")
        set_code(
            job,
            StatusCode.WAITING_ON_REBOOT,
            "Job restarted - waiting for server to reboot",
        )
        # these are idempotent
        docker.kill(container_name(job))
        docker.delete_container(container_name(job))
        get_volume_api(job).delete_volume(job)


def run():
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    args = parser.parse_args()
    main(**vars(args))


if __name__ == "__main__":
    run()
