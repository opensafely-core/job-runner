"""
Ops utility for killing jobs and cleaning up containers and volumes
"""
import argparse

from jobrunner.lib.database import find_where
from jobrunner.manage_jobs import container_name, docker, volume_name
from jobrunner.models import Job, State
from jobrunner.run import mark_job_as_failed


def main(partial_job_ids, cleanup=False):
    jobs = get_jobs(partial_job_ids)
    for job in jobs:
        # If the job has been previously killed we don't want to overwrite the
        # timestamps here
        if job.state in (State.PENDING, State.RUNNING):
            mark_job_as_failed(job, "Killed by admin")
        # All these docker commands are idempotent
        docker.kill(container_name(job))
        if cleanup:
            docker.delete_container(container_name(job))
            docker.delete_volume(volume_name(job))


def get_jobs(partial_job_ids):
    jobs = []
    need_confirmation = False
    for partial_job_id in partial_job_ids:
        matches = find_where(Job, id__like=f"%{partial_job_id}%")
        if len(matches) == 0:
            raise RuntimeError(f"No jobs found matching '{partial_job_id}'")
        elif len(matches) > 1:
            print(f"Multiple jobs found matching '{partial_job_id}':")
            for i, job in enumerate(matches, start=1):
                print(f"  {i}: {job.slug}")
            print()
            index = int(input("Enter number: "))
            assert 0 < index <= len(matches)
            jobs.append(matches[index - 1])
        else:
            need_confirmation = True
            jobs.append(matches[0])
    if need_confirmation:
        print("About to kill jobs:")
        for job in jobs:
            print(f"  {job.slug}")
        confirm = input("\nEnter to continue, Ctrl-C to quit ")
        assert confirm == ""
    return jobs


def run():
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete any associated containers and volumes",
    )
    parser.add_argument(
        "partial_job_ids", nargs="+", help="ID of the job (or substring of the ID)"
    )
    args = parser.parse_args()
    main(**vars(args))


if __name__ == "__main__":
    run()
