"""
Development utility for creating and submitting a JobRequest without having a
job-server
"""
import argparse
import dataclasses
import pprint
import textwrap
from pathlib import Path
from urllib.parse import urlparse

from jobrunner.create_or_update_jobs import create_or_update_jobs
from jobrunner.lib.database import find_where
from jobrunner.lib.git import get_sha_from_remote_ref
from jobrunner.lib.log_utils import configure_logging
from jobrunner.models import Job, random_id
from jobrunner.sync import job_request_from_remote_format


def main(
    repo_url, actions, commit, branch, workspace, database, force_run_dependencies
):
    # Make paths to local repos absolute
    parsed = urlparse(repo_url)
    if not parsed.scheme and not parsed.netloc:
        path = Path(parsed.path).resolve()
        # In case we're on Windows
        repo_url = str(path).replace("\\", "/")
    if not commit:
        commit = get_sha_from_remote_ref(repo_url, branch)
    job_request = job_request_from_remote_format(
        dict(
            identifier=random_id(),
            sha=commit,
            workspace=dict(name=workspace, repo=repo_url, branch=branch, db=database),
            requested_actions=actions,
            force_run_dependencies=force_run_dependencies,
            cancelled_actions=[],
        )
    )
    print("Submitting JobRequest:\n")
    display_obj(job_request)
    create_or_update_jobs(job_request)
    jobs = find_where(Job, job_request_id=job_request.id)
    print(f"Created {len(jobs)} new jobs:\n")
    for job in jobs:
        display_obj(job)


def display_obj(obj):
    if hasattr(obj, "asdict"):
        data = obj.asdict()
    else:
        data = dataclasses.asdict(obj)
    output = pprint.pformat(data)
    print(textwrap.indent(output, "  "))
    print()


def run():
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument("repo_url", help="URL (or local path) of git repository")
    parser.add_argument("actions", nargs="+", help="Name of project action to run")
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
    parser.add_argument(
        "--workspace", help="Workspace ID (default 'test')", default="test"
    )
    parser.add_argument(
        "--database", help="Database name (default 'dummy')", default="dummy"
    )
    parser.add_argument("-f", "--force-run-dependencies", action="store_true")

    args = parser.parse_args()
    main(**vars(args))


if __name__ == "__main__":
    run()
