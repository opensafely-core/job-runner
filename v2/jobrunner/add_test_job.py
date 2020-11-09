"""
Development utility for creating and submittng a JobRequest and then running
the main loop until all jobs have terminated.

Will only submit a new JobRequest if there are no active Jobs in the database.
"""
import argparse
import uuid

from .log_utils import configure_logging
from .sync import job_request_from_remote_format
from .database import find_where
from .models import Job, State
from .create_or_update_jobs import create_or_update_jobs
from . import run


def main(
    repo_url, action, branch, workspace, database, force_run, force_run_dependencies
):
    jobs = find_where(Job, status__in=[State.PENDING, State.RUNNING])
    if jobs:
        print(f"Not adding new JobRequest, found {len(jobs)} active jobs:")
        for job in jobs:
            print(job)
    else:
        job_request = job_request_from_remote_format(
            dict(
                pk=str(uuid.uuid4()),
                workspace=dict(repo=repo_url, branch=branch, db=database),
                workspace_id=workspace,
                action_id=action,
                force_run=force_run,
                force_run_dependencies=force_run_dependencies,
            )
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
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo_url", help="URL (or local path) of git repository")
    parser.add_argument("action", help="Name of project action to run")
    parser.add_argument(
        "--branch", help="Git branch to use (default master)", default="master"
    )
    parser.add_argument("--workspace", help="Workspace ID (default 1)", default="1")
    parser.add_argument(
        "--database", help="Database name (default 'dummy')", default="dummy"
    )
    parser.add_argument("--force-run", action="store_true")
    parser.add_argument("--force-run-dependencies", action="store_true")
    args = parser.parse_args()
    main(**vars(args))
