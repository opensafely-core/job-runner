"""
Script which polls the job-server endpoint for active JobRequests and POSTs
back any associated Jobs.
"""

import logging
import sys
import time

import requests
from opentelemetry import trace

from common import config as common_config
from common.lib.log_utils import configure_logging, set_log_context
from common.tracing import duration_ms_as_span_attr
from controller import config
from controller.create_or_update_jobs import update_cancelled_jobs
from controller.models import JobRequest


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


# This function has been replaced by a call to the RAP API rap/status
# TODO: remove this function & all dependencies (e.g. job_to_remote_format())
def sync_backend_jobs_status(backend, job_requests, span):  # pragma: no cover
    jobs_data = None

    with duration_ms_as_span_attr("api_post.duration_ms", span):
        api_post("jobs", backend=backend, json=jobs_data)


def api_get(*args, backend, **kwargs):
    return api_request("get", *args, backend=backend, **kwargs)


def api_post(*args, backend, **kwargs):  # pragma: no cover
    return api_request("post", *args, backend=backend, **kwargs)


def api_request(method, path, *args, backend, headers=None, **kwargs):
    if headers is None:  # pragma: no cover
        headers = {}

    url = "{}/{}/".format(config.JOB_SERVER_ENDPOINT.rstrip("/"), path.strip("/"))

    if backend not in config.JOB_SERVER_TOKENS:
        raise SyncAPIError(f"No api token found for backend '{backend}'")

    headers["Authorization"] = config.JOB_SERVER_TOKENS[backend]

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


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
