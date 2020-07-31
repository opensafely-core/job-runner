from concurrent.futures import TimeoutError
from pebble import ProcessPool
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging
import os
import requests
import time


from runner.exceptions import OpenSafelyError
from runner.exceptions import DependencyRunning
from runner.utils import get_auth
from runner.utils import getlogger
from runner.job import JobRunner


HOUR = 60 * 60
COHORT_EXTRACTOR_TIMEOUT = 24 * HOUR

logger = getlogger("main")
baselogger = logging.LoggerAdapter(logger, {"job_id": "-"})


def report_result(future):
    jobrunner = future.jobrunner
    job = jobrunner.job
    joblogger = getattr(jobrunner, "logger", baselogger)
    id_message = f"id {jobrunner}"
    try:
        job = future.result(
            timeout=COHORT_EXTRACTOR_TIMEOUT
        )  # blocks until results are ready
        requests.patch(
            job["url"],
            json={
                "status_code": 0,
                "output_bucket": job["output_bucket"],
                "status_message": job["status_message"],
            },
            auth=get_auth(),
        )
        joblogger.info(f"Reported success to job server ({job['status_message']})")
    except TimeoutError as error:
        requests.patch(
            job["url"],
            json={
                "status_code": -1,
                "status_message": f"TimeoutError({COHORT_EXTRACTOR_TIMEOUT}s) {id_message}",
            },
            auth=get_auth(),
        )
        joblogger.info("Reported error -1 (timeout) to job server")
        # Remove pebble's RemoteTraceback exception from reporting
        error.__cause__ = None
        joblogger.exception(error)
    except DependencyRunning as error:
        requests.patch(
            job["url"],
            json={
                "status_code": error.status_code,
                "started": False,
                "status_message": f"{error.safe_details()} {id_message}",
            },
            auth=get_auth(),
        )
        joblogger.info(
            "Reported error %s (%s %s) to job server, and reset the started flag",
            error.status_code,
            error,
            id_message,
        )
    except OpenSafelyError as error:
        requests.patch(
            job["url"],
            json={
                "status_code": error.status_code,
                "status_message": f"{error.safe_details()} {id_message}",
            },
            auth=get_auth(),
        )
        joblogger.info(
            "Reported error %s (%s %s) to job server",
            error.status_code,
            error,
            id_message,
        )
        # Remove pebble's RemoteTraceback exception from reporting
        error.__cause__ = None
        joblogger.exception(error)
    except Exception as error:
        requests.patch(
            job["url"],
            json={
                "status_code": 99,
                "status_message": f"Unclassified error {id_message}",
            },
            auth=get_auth(),
        )
        joblogger.info("Reported error 99 (unclassified) to job server")
        # Don't remove remotetraceback, because we haven't considered
        # handling it explicitly, and the context could help
        joblogger.exception(error)


def check_environment():
    for required_directory in [
        "HIGH_PRIVACY_STORAGE_BASE",
        "MEDIUM_PRIVACY_STORAGE_BASE",
    ]:
        path = os.environ[required_directory]
        assert os.path.exists(
            path,
        ), f"Required directory {path} ({required_directory}) must exist"


def watch(queue_endpoint, loop=True, jobrunner=None):
    check_environment()
    baselogger.info(f"Started watching {queue_endpoint}")
    session = requests.Session()
    # Retries for up to 2 minutes, by default
    retry = Retry(connect=30, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount(queue_endpoint, adapter)
    if jobrunner is None:
        jobrunner = JobRunner
    with ProcessPool(max_tasks=50) as pool:
        while True:
            baselogger.debug(f"Polling {queue_endpoint}")
            try:
                result = session.get(
                    queue_endpoint,
                    params={
                        "started": False,
                        "backend": os.environ["BACKEND"],
                        "page_size": 1,
                    },
                    auth=get_auth(),
                )
            except requests.exceptions.ConnectionError:
                baselogger.exception("Connection error; sleeping for 15 mins")
                time.sleep(60 * 15)
            result.raise_for_status()
            jobs = result.json()
            for job in jobs["results"]:
                response = requests.patch(
                    job["url"], json={"started": True}, auth=get_auth()
                )
                response.raise_for_status()
                runner = jobrunner(job)
                future = pool.schedule(runner, (), timeout=6 * HOUR,)
                future.jobrunner = runner
                future.add_done_callback(report_result)
            if loop:
                time.sleep(os.environ.get("POLL_INTERVAL", 5))
            else:
                break
