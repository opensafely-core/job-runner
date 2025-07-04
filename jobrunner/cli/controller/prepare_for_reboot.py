"""
Ops utility for killing all running jobs and reseting them to PENDING so they will be
automatically re-run after a reboot.
"""

import argparse

from controller.main import cancel_job, set_code
from controller.models import Job, State, StatusCode, Task, TaskType
from controller.queries import get_flag_value
from jobrunner.cli.controller.utils import add_backend_argument
from jobrunner.lib.database import find_where, transaction


def main(backend, status=False, require_confirmation=True):
    # We MUST be paused in order to run prepare-for-reboot, otherwise the
    # controller will just pick tasks right back up again
    paused = str(get_flag_value("paused", backend, default="False")).lower() == "true"
    if not paused and not status:
        print(
            f"\nBackend '{backend}' must be paused in order to prepare for a reboot\n"
            "\n"
            "Pause with:\n"
            f"\tjust jobrunner/pause {backend}\n"
            "\n"
            "Then try again.\n"
        )
        return

    if require_confirmation and not status:
        print(
            "== DANGER ZONE ==\n"
            "\n"
            f"This will kill all running jobs on backend '{backend}' and reset them to the PENDING state, ready\n"
            "to be restarted following a reboot.\n"
            "\n"
            "It should only be run when the job-runner service has been paused on the backend."
            "\n"
        )
        confirm = input("Are you sure you want to continue? (y/N)")
        assert confirm.strip().lower() == "y"

    running_jobs = find_where(Job, state=State.RUNNING, backend=backend)

    if status:
        # Just report on status, don't actually do anything
        report = (
            "\n== PREPARING FOR REBOOT ==\n"
            f"1) backend '{backend}' is {'not ' if not paused else ''}paused\n"
            f"2) {len(running_jobs)} job(s) are running\n"
        )
        cancel_tasks = find_where(
            Task, type=TaskType.CANCELJOB, backend=backend, active=True
        )
        if cancel_tasks:
            report += f"3) {len(cancel_tasks)} job(s) are being cancelled\n"

        if not running_jobs and not cancel_tasks:
            # No jobs are running, and there are no active canceljob tasks, we are ready to reboot
            report += "\n== READY TO REBOOT ==\n"
            if paused:
                report += "Safe to reboot now\n"
            else:
                report += f"Pause backend '{backend}' before rebooting\n"

        print(report)
        return

    for job in running_jobs:
        print(f"resetting job {job.slug} to PENDING")
        with transaction():
            set_code(
                job,
                StatusCode.WAITING_ON_REBOOT,
                "Job restarted - waiting for server to reboot",
            )

            print(f"Killing job {job.slug}")
            cancel_job(job)


def add_parser_args(parser):
    add_backend_argument(parser)
    parser.add_argument(
        "-s",
        "--status",
        action="store_true",
        default=False,
        help="Report on status of system in prepration for reboot",
    )


def run():  # pragma: no cover
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    add_parser_args(parser)
    args = parser.parse_args()
    main(**vars(args))


if __name__ == "__main__":
    run()  # pragma: no cover
