"""
Ops utility for retrying `finalise_job` on a job which hit an error during
completion

This only applies to (and only works for) jobs which failed with an
Internal Error during the "finalise" step where we copy output files,
dump logs, update the manifest etc. When this happens we don't
automatically clean up the job's container and volume giving us an
opportunity to fix the bug and try again without having to re-run the
entire job.

To do this we simply put the job back into the RUNNING state and let the
jobrunner pick it up again. We also need to update the job-server when we do
this so that it puts the job back in an "active" state and continues to ask for
updates on it.
"""
import argparse
import time

from .sync import api_post, job_to_remote_format
from .database import find_where, update
from .models import Job, State
from .manage_jobs import docker, container_name


def main(partial_job_id):
    job = get_job(partial_job_id)
    if not docker.container_exists(container_name(job)):
        raise RuntimeError("Cannot reset job, associated container does not exist")
    job.state = State.RUNNING
    job.status_message = "Re-attempting to extract outputs"
    job.status_code = None
    job.completed_at = None
    job.updated_at = int(time.time())
    print("\nUpdating job in database:")
    print(job)
    update(
        job,
        update_fields=[
            "state",
            "status_message",
            "status_code",
            "completed_at",
            "updated_at",
        ],
    )
    print("\nPOSTing update to job-server")
    api_post("jobs", json=[job_to_remote_format(job)])
    print("\nDone")


def get_job(partial_job_id):
    matches = find_where(Job, id__like=f"%{partial_job_id}%")
    if len(matches) == 0:
        raise RuntimeError("No matching jobs found")
    elif len(matches) > 1:
        print("Multiple matching jobs found:")
        for i, job in enumerate(matches, start=1):
            print(f"  {i}: {job.slug}")
        print()
        index = int(input("Enter number: "))
        assert 0 < index <= len(matches)
        job = matches[index - 1]
    else:
        job = matches[0]
        print(f"About to reset job:\n  {job.slug}\n")
        confirm = input("Enter to continue, Ctrl-C to quit ")
        assert confirm == ""
    return job


def run():
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument("partial_job_id", help="ID of the job (or substring of the ID)")
    args = parser.parse_args()
    main(**vars(args))


if __name__ == "__main__":
    run()
