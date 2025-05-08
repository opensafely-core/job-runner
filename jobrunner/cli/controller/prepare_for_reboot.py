"""
Ops utility for killing all running jobs and reseting them to PENDING so they will be
automatically re-run after a reboot.
"""

import argparse

from jobrunner.cli.controller.utils import add_backend_argument
from jobrunner.controller.main import cancel_job, set_code
from jobrunner.lib.database import find_where, transaction
from jobrunner.models import Job, State, StatusCode


def main(backend, require_confirmation=True):
    if require_confirmation:
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

    for job in find_where(Job, state=State.RUNNING, backend=backend):
        print(f"resetting job {job.slug} to PENDING")
        with transaction():
            set_code(
                job,
                StatusCode.WAITING_ON_REBOOT,
                "Job restarted - waiting for server to reboot",
            )

            print(f"Killing job {job.slug}")
            cancel_job(job)


def run():  # pragma: no cover
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    add_backend_argument(parser)
    args = parser.parse_args()
    main(**vars(args))


if __name__ == "__main__":
    run()  # pragma: no cover
