"""
Script which polls the job-server endpoint for active JobRequests and POSTs
back any associated Jobs.
"""
import time

import requests

from . import config
from .jobrequest import create_or_update_jobs
from .database import find_where


session = requests.Session()


def main():
    while True:
        sync()
        time.sleep(config.POLL_INTERVAL)


def sync():
    job_requests = api_get(
        "job-requests", params={"active": "true", "backend": config.BACKEND}
    )
    for job_request in job_requests:
        create_or_update_jobs(job_request)
    job_request_ids = [i["pk"] for i in job_requests]
    jobs = find_where("job", job_request_id__in=job_request_ids)
    # TODO: We'll want to apply some kind of translation layer here, rather
    # than just reflecting the exact structure of our database table as JSON
    api_post("jobs", json=jobs)


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


if __name__ == "__main__":
    main()
