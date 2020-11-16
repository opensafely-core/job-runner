"""
Script which polls the job-server endpoint for active JobRequests and POSTs
back any associated Jobs.
"""
import time

import requests

from .log_utils import configure_logging, set_log_context
from . import config
from .create_or_update_jobs import create_or_update_jobs
from .database import find_where
from .models import JobRequest, Job
from .string_utils import slugify, project_name_from_url


session = requests.Session()


def main():
    while True:
        sync()
        time.sleep(config.POLL_INTERVAL)


def sync():
    results = api_get(
        "job-requests",
        # We're deliberately not paginating here on the assumption that the set
        # of active jobs is always going to be small enough that we can fetch
        # them in a single request and we don't need the extra complexity
        params={"active": "true", "backend": config.BACKEND},
    )
    job_requests = [job_request_from_remote_format(i) for i in results]
    job_request_ids = [i.id for i in job_requests]
    for job_request in job_requests:
        with set_log_context(job_request=job_request):
            create_or_update_jobs(job_request)
    jobs = find_where(Job, job_request_id__in=job_request_ids)
    jobs_data = [job_to_remote_format(i) for i in jobs]
    api_post("jobs", json=jobs_data)


def api_get(*args, **kwargs):
    return api_request("get", *args, **kwargs)


def api_post(*args, **kwargs):
    return api_request("post", *args, **kwargs)


def api_request(method, path, *args, **kwargs):
    url = "{}/{}".format(config.JOB_SERVER_ENDPOINT.rstrip("/"), path.lstrip("/"))
    # We could do this just once on import, but it makes changing the config in
    # tests more fiddly
    session.auth = (config.QUEUE_USER, config.QUEUE_PASS)
    response = session.request(method, url, *args, **kwargs)
    response.raise_for_status()
    return response.json()


def job_request_from_remote_format(job_request):
    """
    Convert a JobRequest as received from the job-server into our own internal
    representation
    """
    return JobRequest(
        id=str(job_request["id"]),
        repo_url=job_request["workspace"]["repo"],
        commit=job_request.get("sha"),
        branch=job_request["workspace"]["branch"],
        requested_actions=job_request["requested_actions"],
        workspace=generate_workspace_slug(job_request),
        database_name=job_request["workspace"]["db"],
        force_run_dependencies=job_request["force_run_dependencies"],
        original=job_request,
    )


def generate_workspace_slug(job_request):
    """
    We identify workspaces internally using a slugs which look something like this:

        some-study-123
        some-study-daves-branch-145

    These slugs are used as the directory name for the workspace output and
    also appear as part of job names, log directories etc. The workspace ID at
    the end ensures uniqueness so we just need to ensure that these are helpful
    for a human eyeballing a log file or directory listing.
    """
    repo_url = job_request["workspace"]["repo"]
    branch = job_request["workspace"]["branch"]
    database_name = job_request["workspace"]["db"]
    workspace_id = job_request["workspace"]["id"]
    parts = [project_name_from_url(repo_url)]
    # Only include the branch if it's not the default to minimise clutter. (We
    # include HEAD here only because it's useful in local testing.)
    if branch and branch not in ["master", "main", "HEAD"]:
        parts.append(branch)
    # Only include the database name if it's not the default to minimise
    # clutter
    if database_name != "full" and not config.USING_DUMMY_DATA_BACKEND:
        parts.append(database_name)
    parts.append(str(workspace_id))
    return slugify("-".join(parts))


def job_to_remote_format(job):
    """
    Convert our internal representation of a Job into whatever format the
    job-server expects
    """
    return {
        key: value
        for (key, value) in job.asdict().items()
        if key
        in [
            "id",
            "job_request_id",
            "action",
            "status",
            "status_message",
            "created_at",
            "updated_at",
            "started_at",
            "completed_at",
        ]
    }


if __name__ == "__main__":
    configure_logging()
    main()
