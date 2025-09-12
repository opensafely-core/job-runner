"""
Script which polls the job-server endpoint for active JobRequests and POSTs
back any associated Jobs.
"""

import json
import logging
import sys
import time

import requests
from opentelemetry import trace

from common import config as common_config
from common.lib.log_utils import configure_logging, set_log_context
from common.tracing import duration_ms_as_span_attr
from controller import config, queries
from controller.create_or_update_jobs import update_cancelled_jobs
from controller.lib.database import find_where, select_values
from controller.main import get_task_for_job
from controller.models import Job, JobRequest, State


session = requests.Session()
log = logging.getLogger(__name__)
tracer = trace.get_tracer("sync")


class SyncAPIError(Exception):
    pass


def main():  # pragma: no cover
    log.info(
        f"Polling for JobRequests at: "
        f"{config.JOB_SERVER_ENDPOINT.rstrip('/')}/job-requests/"
    )
    while True:
        sync()
        time.sleep(config.POLL_INTERVAL)


def sync():
    for backend in common_config.BACKENDS:
        sync_backend(backend)


def sync_backend(backend):
    with tracer.start_as_current_span(
        "sync_backend", attributes={"backend": backend}
    ) as span:
        with duration_ms_as_span_attr("api_get.duration_ms", span):
            response = api_get(
                "job-requests",
                backend=backend,
                # We're deliberately not paginating here on the assumption that the set
                # of active jobs is always going to be small enough that we can fetch
                # them in a single request and we don't need the extra complexity
            )
        with duration_ms_as_span_attr("parse_requests.duration_ms", span):
            job_requests = [
                job_request_from_remote_format(i) for i in response["results"]
            ]

        # Bail early if there's nothing to do
        if not job_requests:
            return

        with duration_ms_as_span_attr("create.duration_ms", span):
            for job_request in job_requests:
                with set_log_context(job_request=job_request):
                    update_cancelled_jobs(job_request)

        # Temporarily disable status part of sync loop for the test backend
        # Part of RAP API step 2 work
        if backend == "test":
            return

        sync_backend_jobs_status(backend, job_requests, span)


# TODO: this function will be replaced by a call to the RAP API rap/status
def sync_backend_jobs_status(backend, job_requests, span):
    with duration_ms_as_span_attr("find_ids.duration_ms", span):
        job_request_ids = [i.id for i in job_requests]

    # `job_request_ids` contains all the JobRequests which job-server thinks are
    # active; this query gets all those which _we_ think are active
    with duration_ms_as_span_attr("find_more_ids.duration_ms", span):
        active_job_request_ids = select_values(
            Job, "job_request_id", state__in=[State.PENDING, State.RUNNING]
        )
        # We sync all jobs belonging to either set (using `dict.fromkeys` to preserve order
        # for easier testing)
        job_request_ids_to_sync = list(
            dict.fromkeys(job_request_ids + active_job_request_ids)
        )
    with duration_ms_as_span_attr("find_where.duration_ms", span):
        jobs = find_where(Job, job_request_id__in=job_request_ids_to_sync)
    with duration_ms_as_span_attr("encode_jobs.duration_ms", span):
        jobs_data = [job_to_remote_format(i) for i in jobs]
    log.debug(f"Syncing {len(jobs_data)} jobs back to job-server")

    with duration_ms_as_span_attr("api_post.duration_ms", span):
        api_post("jobs", backend=backend, json=jobs_data)


def api_get(*args, backend, **kwargs):
    return api_request("get", *args, backend=backend, **kwargs)


def api_post(*args, backend, **kwargs):
    return api_request("post", *args, backend=backend, **kwargs)


def api_request(method, path, *args, backend, headers=None, **kwargs):
    if headers is None:  # pragma: no cover
        headers = {}

    url = "{}/{}/".format(config.JOB_SERVER_ENDPOINT.rstrip("/"), path.strip("/"))

    if backend not in config.JOB_SERVER_TOKENS:
        raise SyncAPIError(f"No api token found for backend '{backend}'")

    flags = {
        f.id: {"v": f.value, "ts": f.timestamp_isoformat}
        for f in queries.get_current_flags(backend=backend)
    }

    headers["Authorization"] = config.JOB_SERVER_TOKENS[backend]
    headers["Flags"] = json.dumps(flags, separators=(",", ":"))

    response = session.request(method, url, *args, headers=headers, **kwargs)

    log.debug(
        "{} {} {} post_data={} {}".format(
            method.upper(),
            response.status_code,
            url,
            kwargs.get("json", '""'),
            response.text,
        )
    )

    try:
        response.raise_for_status()
    except Exception as e:  # pragma: no cover
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
        codelists_ok=job_request["codelists_ok"],
        database_name=job_request["database_name"],
        force_run_dependencies=job_request["force_run_dependencies"],
        backend=job_request["backend"],
        original=job_request,
    )


def job_to_remote_format(job):
    """
    Convert our internal representation of a Job into whatever format the
    job-server expects
    """

    metrics = {}
    if task := get_task_for_job(job):
        if task.agent_results:
            metrics = task.agent_results.get("job_metrics", {})

    return {
        "identifier": job.id,
        "job_request_id": job.job_request_id,
        "action": job.action,
        "run_command": job.run_command,
        "status": job.state.value,
        "status_code": job.status_code.value,
        "status_message": job.status_message or "",
        "created_at": job.created_at_isoformat,
        "updated_at": job.updated_at_isoformat,
        "started_at": job.started_at_isoformat,
        "completed_at": job.completed_at_isoformat,
        "trace_context": job.trace_context,
        "metrics": metrics,
        "requires_db": job.requires_db,
    }


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
