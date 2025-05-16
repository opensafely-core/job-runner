"""
Ops utility for killing run-job tasks

Kills and cleans up running containers matching provided
job ids. Will accept and attempt to match partial job ids.

Killing the task will cause the agent to retry.
"""

import argparse
import re
import sys
from dataclasses import dataclass

from jobrunner.executors.volumes import delete_volume
from jobrunner.lib import docker


@dataclass(frozen=True)
class JobDefinitionId:
    id: str


def main(partial_job_ids):
    # Get all running job containers that match the partial job ids provided
    job_containers = get_job_containers(partial_job_ids)
    for job_container in job_containers:
        job_id = job_id_from_container_name(job_container)
        # check it exists, it might have finished since we got the list of running containers
        if not docker.container_exists(job_container):
            print(
                f"Cannot kill task for job {job_id}, associated container does not exist. "
                "The job may have completed."
            )
            continue

        docker.kill(job_container)
        docker.delete_container(job_container)
        delete_volume(JobDefinitionId(id=job_id))
        print(f"Task for job {job_id} killed.")


def get_job_containers(partial_job_ids):
    # We don't have access to the jobs database in this (agent) command, so we need
    # to use docker to find all jobs with a container that exists
    # get all job container names

    job_container_names = get_container_names()
    # Use a dict to track the containers
    # This keeps the order we match them, but excludes duplicates in case
    # multiple partial ids match the same containers
    job_containers = {}
    need_confirmation = False
    for partial_job_id in partial_job_ids:
        # look for partial matches
        partial_matches = list(matching_containers(partial_job_id, job_container_names))

        if len(partial_matches) == 0:
            print(f"No running tasks found matching '{partial_job_id}'")
        elif len(partial_matches) > 1:
            print(f"Multiple running tasks found matching '{partial_job_id}':")
            for i, job_container in enumerate(partial_matches, start=1):
                print(f"  {i}: {job_id_from_container_name(job_container)}")
            print()
            index = int(input("Enter number: "))
            assert 0 < index <= len(partial_matches)
            job_containers[partial_matches[index - 1]] = None
        else:
            # We only need confirmation if the supplied job ID doesn't exactly
            # match the found job
            job_container = partial_matches[0]
            if job_id_from_container_name(job_container) != partial_job_id:
                need_confirmation = True

            job_containers[job_container] = None

    job_containers = list(job_containers)
    if need_confirmation:
        print("About to kill tasks for jobs:")
        for job_container in job_containers:
            print(f"  {job_id_from_container_name(job_container)}")
        confirm = input("\nEnter to continue, Ctrl-C to quit ")
        assert confirm == ""
    return job_containers


def job_id_from_container_name(job_container_name):
    return job_container_name.lstrip("os-job-")


def get_container_names():
    response = docker.docker(
        [
            "container",
            "ls",
            "--filter",
            "name=os-job-",
            "--format",
            "'{{ json .Names }}'",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return re.findall(r"\"(os-job-.+)\"", response.stdout)


def matching_containers(partial_job_id, container_names):
    for job_container_name in container_names:
        if partial_job_id in job_container_name:
            yield job_container_name


def run(argv):
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument(
        "partial_job_ids", nargs="+", help="ID of the jobs (or substring of the ID)"
    )
    args = parser.parse_args(argv)
    main(**vars(args))


if __name__ == "__main__":
    run(sys.argv[1:])
