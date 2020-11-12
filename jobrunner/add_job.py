"""
Development utility for creating and submittng a JobRequest (and optionally
running the main loop until all jobs have terminated).
"""
import argparse
import dataclasses
import pprint
from pathlib import Path
from urllib.parse import urlparse
import textwrap

from .log_utils import configure_logging
from .sync import job_request_from_remote_format
from .database import find_where
from .models import Job, State
from .create_or_update_jobs import create_or_update_jobs
from .manage_jobs import high_privacy_output_dir
from . import config
from . import run


def main(run=False, **kwargs):
    if run:
        if not config.USING_DUMMY_DATA_BACKEND:
            raise RuntimeError("--run flag can only be used on dummy data backend")
        active_jobs = find_where(Job, status__in=[State.PENDING, State.RUNNING])
        if active_jobs:
            print(f"Not adding new JobRequest, found {len(active_jobs)} active jobs:\n")
            for job in active_jobs:
                display_obj(job)
        else:
            submit_job_request(**kwargs)
        run_main_loop()
    else:
        submit_job_request(**kwargs)


def submit_job_request(
    repo_url, action, commit, branch, workspace, database, force_run_dependencies
):
    parsed = urlparse(repo_url)
    if not parsed.scheme and not parsed.netloc:
        path = Path(parsed.path).resolve()
        # In case we're on Windows
        repo_url = str(path).replace("\\", "/")
    job_request = job_request_from_remote_format(
        dict(
            pk=Job.new_id(),
            workspace=dict(repo=repo_url, branch=branch, db=database),
            workspace_id=workspace,
            action_id=action,
            force_run=True,
            force_run_dependencies=force_run_dependencies,
        )
    )
    print("Submitting JobRequest:\n")
    display_obj(job_request)
    create_or_update_jobs(job_request)
    jobs = find_where(Job, job_request_id=job_request.id)
    print(f"Created {len(jobs)} new jobs:\n")
    for job in jobs:
        display_obj(job)


def run_main_loop():
    active_jobs = find_where(Job, status__in=[State.PENDING, State.RUNNING])
    print("Running jobrunner.run loop")
    run.main(exit_when_done=True)
    final_jobs = find_where(Job, id__in=[job.id for job in active_jobs])
    print("\nOutputs, logs etc can be found in the below directories:\n")
    for job in final_jobs:
        print(f"=> {job.action}")
        print(f"   {job.status_message}")
        print(f"   {high_privacy_output_dir(job)}")
        print()


def display_obj(obj):
    if hasattr(obj, "asdict"):
        data = obj.asdict()
    else:
        data = dataclasses.asdict(obj)
    output = pprint.pformat(data)
    print(textwrap.indent(output, "  "))
    print()


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo_url", help="URL (or local path) of git repository")
    parser.add_argument("action", help="Name of project action to run")
    parser.add_argument(
        "--commit",
        help=(
            "Git commit to use (if repo_url is a local checkout, use current "
            "checked out commit by default)"
        ),
    )
    parser.add_argument(
        "--branch",
        help="Git branch or ref to use if no commit supplied (default HEAD)",
        default="HEAD",
    )
    parser.add_argument("--workspace", help="Workspace ID (default 1)", default="1")
    parser.add_argument(
        "--database", help="Database name (default 'dummy')", default="dummy"
    )
    parser.add_argument("-f", "--force-run-dependencies", action="store_true")
    parser.add_argument(
        "--run",
        help=(
            "Run the main loop until all jobs are terminated (will only run on "
            "dummy data backend)"
        ),
        action="store_true",
    )

    args = parser.parse_args()
    main(**vars(args))
