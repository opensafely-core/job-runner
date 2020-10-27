"""
Development utility for creating and submittng a JobRequest and then running
the main loop until all jobs have terminated.

Will only submit a new JobRequest if there are no active Jobs in the database.
"""
import argparse
import uuid

from .sys_excepthook import add_excepthook
from .database import find_where
from .models import JobRequest, Job, State
from .create_or_update_jobs import create_or_update_jobs
from . import run


def main(repo_url, action, branch, workspace):
    jobs = find_where(Job, status__in=[State.PENDING, State.RUNNING])
    if jobs:
        print(f"Not adding new JobRequest, found {len(jobs)} active jobs:")
        for job in jobs:
            print(job)
    else:
        job_request = JobRequest(
            id=str(uuid.uuid4()),
            repo_url=repo_url,
            commit=None,
            branch=branch,
            action=action,
            workspace=workspace,
            original={},
        )
        print(f"Submitting JobRequest: {job_request}")
        create_or_update_jobs(job_request)
        jobs = find_where(Job, job_request_id=job_request.id)
        print(f"Created {len(jobs)} new jobs:")
        for job in jobs:
            print(job)
    print("Running jobrunner.run loop")
    run.main(exit_when_done=True)
    final_jobs = find_where(Job, id__in=[job.id for job in jobs])
    print("Final jobs:")
    for job in final_jobs:
        print(job)


if __name__ == "__main__":
    add_excepthook()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo_url", help="URL (or local path) of git repository")
    parser.add_argument("action", help="Name of project action to run")
    parser.add_argument(
        "--branch", help="Git branch to use (default master)", default="master"
    )
    parser.add_argument("--workspace", help="Workspace ID (default 1)", default="1")
    args = parser.parse_args()
    main(**vars(args))
