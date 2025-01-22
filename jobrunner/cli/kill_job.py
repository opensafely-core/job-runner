"""
Ops utility for killing jobs and cleaning up containers and volumes
"""

import argparse

from jobrunner.executors import local
from jobrunner.job_executor import JobResults
from jobrunner.lib import database, docker
from jobrunner.models import Job, State, StatusCode
from jobrunner.run import job_to_job_definition, mark_job_as_failed


def main(partial_job_ids, cleanup=False):
    jobs = get_jobs(partial_job_ids)
    #
    #
    for job in jobs:
        # If the job has been previously killed we don't want to overwrite the
        # timestamps here
        container = local.container_name(job)
        if job.state in (State.PENDING, State.RUNNING):
            mark_job_as_failed(
                job,
                StatusCode.KILLED_BY_ADMIN,
                "An OpenSAFELY admin manually killed this job",
            )
        # All these docker commands are idempotent
        docker.kill(container)

        # save the logs
        container_metadata = docker.container_inspect(
            container, none_if_not_exists=True
        )
        if container_metadata:
            job = job_to_job_definition(job)
            # create a dummy JobResults with just the message we want
            results = JobResults(
                outputs=None,
                unmatched_patterns=None,
                unmatched_outputs=None,
                exit_code=container_metadata["State"]["ExitCode"],
                image_id=container_metadata["Image"],
                message="job killed by OpenSAFELY administrator",
            )
            metadata = local.get_job_metadata(job, {}, container_metadata, results)
            local.write_job_logs(job, metadata, copy_log_to_workspace=False)

        if cleanup:
            docker.delete_container(container)
            # nb. `job` could potentially be a Job or a JobDefinition
            local.volumes.get_volume_api(job).delete_volume(job)


def get_jobs(partial_job_ids):
    jobs = []
    need_confirmation = False
    for partial_job_id in partial_job_ids:
        # look for partial matches
        partial_matches = database.find_where(Job, id__like=f"%{partial_job_id}%")
        if len(partial_matches) == 0:
            raise RuntimeError(f"No jobs found matching '{partial_job_id}'")
        elif len(partial_matches) > 1:
            print(f"Multiple jobs found matching '{partial_job_id}':")
            for i, job in enumerate(partial_matches, start=1):
                print(f"  {i}: {job.slug}")
            print()
            index = int(input("Enter number: "))
            assert 0 < index <= len(partial_matches)
            jobs.append(partial_matches[index - 1])
        else:
            # We only need confirmation if the supplied job ID doesn't exactly
            # match the found job
            job = partial_matches[0]
            if job.id != partial_job_id:
                need_confirmation = True
            jobs.append(job)
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
