"""
Development utility for creating and submitting a JobRequest without having a
job-server
"""

import argparse
import dataclasses
import pprint
import sys
import textwrap
from pathlib import Path
from urllib.parse import urlparse

from common import tracing
from common.lib.git import get_sha_from_remote_ref
from common.lib.log_utils import configure_logging
from controller.cli.utils import add_backend_argument
from controller.create_or_update_jobs import create_jobs
from controller.lib.database import find_where
from controller.models import Job, random_id
from controller.webapp.views.validators.dataclasses import CreateRequest


def main(
    repo_url,
    actions,
    backend,
    commit,
    branch,
    workspace,
    database,
    force_run_dependencies,
):
    tracing.setup_default_tracing("controller")
    # Make paths to local repos absolute
    parsed = urlparse(repo_url)
    if not parsed.scheme and not parsed.netloc:  # pragma: no cover
        path = Path(parsed.path).resolve()
        repo_url = str(path)
    if not commit:
        commit = get_sha_from_remote_ref(repo_url, branch)
    create_request = _make_create_request(
        dict(
            identifier=random_id(),
            sha=commit,
            database_name=database,
            workspace=dict(name=workspace, repo=repo_url, branch=branch),
            requested_actions=actions,
            force_run_dependencies=force_run_dependencies,
            codelists_ok=True,
            backend=backend,
            created_by="controller",
            project="unknown",
            orgs=[],
        )
    )
    print("Submitting JobRequest:\n")
    display_obj(create_request)
    create_jobs(create_request)
    jobs = find_where(Job, rap_id=create_request.id)
    print(f"Created {len(jobs)} new jobs:\n")
    for job in jobs:
        display_obj(job)

    return create_request, jobs


def display_obj(obj):
    if hasattr(obj, "asdict"):
        data = obj.asdict()
    else:
        data = dataclasses.asdict(obj)
    output = pprint.pformat(data)
    print(textwrap.indent(output, "  "))
    print()


def _make_create_request(args_dict: dict) -> CreateRequest:
    """
    Construct an object suitable for passing to create_jobs
    """
    return CreateRequest(
        id=str(args_dict["identifier"]),
        backend=args_dict["backend"],
        workspace=args_dict["workspace"]["name"],
        repo_url=args_dict["workspace"]["repo"],
        branch=args_dict["workspace"]["branch"],
        commit=args_dict["sha"],
        database_name=args_dict["database_name"],
        requested_actions=args_dict["requested_actions"],
        codelists_ok=args_dict["codelists_ok"],
        force_run_dependencies=args_dict["force_run_dependencies"],
        created_by=args_dict["created_by"],
        project=args_dict["project"],
        orgs=args_dict["orgs"],
        original=args_dict,
    )


def add_parser_args(parser):
    parser.add_argument("repo_url", help="URL (or local path) of git repository")
    parser.add_argument("actions", nargs="+", help="Name of project action to run")
    add_backend_argument(parser)
    parser.add_argument(
        "--commit",
        help=(
            "Git commit to use (if repo_url is a local checkout, use current "
            "checked out commit by default)"
        ),
    )
    parser.add_argument(
        "--branch",
        help="Git branch or ref to use if no commit supplied (default 'main')",
        default="main",
    )
    parser.add_argument(
        "--workspace", help="Workspace ID (default 'test')", default="test"
    )
    parser.add_argument(
        "--database", help="Database name (default 'default')", default="default"
    )
    parser.add_argument("-f", "--force-run-dependencies", action="store_true")


def run(argv=None):
    if argv is None:  # pragma: no cover
        argv = sys.argv[1:]

    configure_logging()

    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    add_parser_args(parser)
    args = parser.parse_args(argv)
    return main(**vars(args))


if __name__ == "__main__":
    run()  # pragma: no cover
