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
from .git import name_from_repo_url
from .models import JobRequest, Job
from .string_utils import slugify


session = requests.Session()


def main():
    while True:
        sync()
        time.sleep(config.POLL_INTERVAL)


def sync():
    results = api_get(
        "job-requests", params={"active": "true", "backend": config.BACKEND}
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
        id=str(job_request["pk"]),
        repo_url=job_request["workspace"]["repo"],
        commit=job_request.get("commit"),
        branch=job_request["workspace"]["branch"],
        action=job_request["action_id"],
        workspace=generate_workspace_name(job_request),
        database_name=job_request["workspace"]["db"],
        force_run=job_request["force_run"],
        force_run_dependencies=job_request["force_run_dependencies"],
        original=job_request,
    )


def generate_workspace_name(job_request):
    repo_url = job_request["workspace"]["repo"]
    branch = job_request["workspace"]["branch"]
    database_name = job_request["workspace"]["db"]
    workspace_id = job_request["workspace_id"]
    parts = [name_from_repo_url(repo_url)]
    if branch not in ["master", "main"]:
        parts.append(branch)
    if database_name != "full" and not config.USING_DUMMY_DATA_BACKEND:
        parts.append(database_name)
    parts.append(workspace_id)
    return slugify("-".join(parts))


def job_to_remote_format(job):
    """
    Convert our internal representation of a Job into whatever format the
    job-server expects
    """
    # TODO: Work out what we need to do here
    return job.asdict()


if __name__ == "__main__":
    configure_logging()
    main()
