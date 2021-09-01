"""
Script which polls the job-server endpoint for active JobRequests and POSTs
back any associated Jobs.
"""
import logging
import sys
import time

import requests

from jobrunner import config
from jobrunner.create_or_update_jobs import create_or_update_jobs
from jobrunner.lib.database import find_where
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.models import Job, JobRequest

session = requests.Session()
log = logging.getLogger(__name__)


class SyncAPIError(Exception):
    pass


def main():
    log.info(
        f"Polling for JobRequests at: "
        f"{config.JOB_SERVER_ENDPOINT.rstrip('/')}/job-requests/"
    )
    while True:
        sync()
        time.sleep(config.POLL_INTERVAL)


def sync():
    response = api_get(
        "job-requests",
        # We're deliberately not paginating here on the assumption that the set
        # of active jobs is always going to be small enough that we can fetch
        # them in a single request and we don't need the extra complexity
        params={"backend": config.BACKEND},
    )
    job_requests = [job_request_from_remote_format(i) for i in response["results"]]

    # Bail early if there's nothing to do
    if not job_requests:
        return

    job_request_ids = [i.id for i in job_requests]
    for job_request in job_requests:
        with set_log_context(job_request=job_request):
            create_or_update_jobs(job_request)
    jobs = find_where(Job, job_request_id__in=job_request_ids)
    jobs_data = [job_to_remote_format(i) for i in jobs]
    log.debug(f"Syncing {len(jobs_data)} jobs back to job-server")

    api_post("jobs", json=jobs_data)


def api_get(*args, **kwargs):
    return api_request("get", *args, **kwargs)


def api_post(*args, **kwargs):
    return api_request("post", *args, **kwargs)


def api_request(method, path, *args, **kwargs):
    url = "{}/{}/".format(config.JOB_SERVER_ENDPOINT.rstrip("/"), path.strip("/"))
    # We could do this just once on import, but it makes changing the config in
    # tests more fiddly
    session.headers = {"Authorization": config.JOB_SERVER_TOKEN}
    response = session.request(method, url, *args, **kwargs)

    log.debug(
        "%s %s %s post_data=%s %s"
        % (
            method.upper(),
            response.status_code,
            url,
            kwargs.get("json", '""'),
            response.text,
        )
    )

    try:
        response.raise_for_status()
    except Exception as e:
        raise SyncAPIError(e) from e

    return response.json()


def job_request_from_remote_format(job_request):
    """
    Convert a JobRequest as received from the job-server into our own internal
    representation
    """
    return JobRequest(
        id=str(job_request["identifier"]),
        repo_url=job_request["workspace"]["repo"],
        commit=job_request["sha"],
        branch=job_request["workspace"]["branch"],
        requested_actions=job_request["requested_actions"],
        cancelled_actions=job_request["cancelled_actions"],
        workspace=job_request["workspace"]["name"],
        database_name=job_request["workspace"]["db"],
        force_run_dependencies=job_request["force_run_dependencies"],
        original=job_request,
    )


def job_to_remote_format(job):
    """
    Convert our internal representation of a Job into whatever format the
    job-server expects
    """
    return {
        "identifier": job.id,
        "job_request_id": job.job_request_id,
        "action": job.action,
        "status": job.state.value,
        "status_code": job.status_code.value if job.status_code else "",
        "status_message": job.status_message or "",
        "created_at": job.created_at_isoformat,
        "updated_at": job.updated_at_isoformat,
        "started_at": job.started_at_isoformat,
        "completed_at": job.completed_at_isoformat,
    }


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
