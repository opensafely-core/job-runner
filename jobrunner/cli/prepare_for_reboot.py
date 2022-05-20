"""
Ops utility for killing all running jobs and reseting them to PENDING so they will be
automatically re-run after a reboot.
"""
import argparse

from jobrunner.executor.local import container_name, docker, volume_name
from jobrunner.lib.database import find_where, update_where
from jobrunner.models import Job, State


def main():
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
    # Reset all running jobs to pending
    update_where(Job, {"state": State.PENDING, "started_at": None}, state=State.RUNNING)
    # Make sure all containers and volumes are removed ready to freshly restart the jobs
    # after the reboot
    for job in find_where(Job, state=State.PENDING):
        docker.kill(container_name(job))
        docker.delete_container(container_name(job))
        docker.delete_volume(volume_name(job))


def run():
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    args = parser.parse_args()
    main(**vars(args))


if __name__ == "__main__":
    run()
