import logging
import os
import subprocess
import time
from concurrent.futures import TimeoutError
from multiprocessing import cpu_count

import requests
from pebble import ProcessPool
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from jobrunner import utils
from jobrunner.exceptions import DependencyRunning, OpenSafelyError
from jobrunner.job import Job

HOUR = 60 * 60
COHORT_EXTRACTOR_TIMEOUT = 24 * HOUR

logger = utils.getlogger("main")
baselogger = logging.LoggerAdapter(logger, {"job_id": "-"})


def report_result(future):
    """A pebble callback that is called when results are ready *or* there's an error
    """
    jobrunner = future.jobrunner
    job_spec = jobrunner.job_spec
    joblogger = getattr(jobrunner, "logger", baselogger)
    joblogger.info("Started pebble's `add_done_callback`")
    id_message = f"id {jobrunner}"
    try:
        jobs = future.result()
        assert len(jobs) == 1
        job = future.result()[0]
        outputs = [{"location": x["base_path"]} for x in job["output_locations"]]
        response = requests.patch(
            job["url"],
            json={
                "status_code": 0,
                "outputs": outputs,
                "status_message": job["status_message"],
            },
            auth=utils.get_auth(),
        )
        if not response.ok:
            joblogger.error("Problem updating job: %s", response.text)
        response.raise_for_status()
        joblogger.info(f"Reported success to job server ({job['status_message']})")
    except TimeoutError as error:
        response = requests.patch(
            job_spec["url"],
            json={
                "status_code": -1,
                "status_message": f"TimeoutError({COHORT_EXTRACTOR_TIMEOUT}s) {id_message}",
            },
            auth=utils.get_auth(),
        )
        response.raise_for_status()
        joblogger.info("Reported error -1 (timeout) to job server")
        # Remove pebble's RemoteTraceback exception from reporting
        error.__cause__ = None
        joblogger.exception(error)
    except DependencyRunning as error:
        # Because the error is simply that we're not yet ready, reset
        # the `started` flag so that our main loop gets the chance to
        # try re-running the action in a future iteration
        payload = {
            "status_code": error.status_code,
            "started": False,
            "status_message": f"{error.safe_details()} {id_message}",
        }
        response = requests.patch(job_spec["url"], json=payload, auth=utils.get_auth())
        response.raise_for_status()

        joblogger.info(
            "Reported error %s (%s %s) to job server, and reset the started flag",
            error.status_code,
            error,
            id_message,
        )
    except OpenSafelyError as error:
        response = requests.patch(
            job_spec["url"],
            json={
                "status_code": error.status_code,
                "status_message": f"{error.safe_details()} {id_message}",
            },
            auth=utils.get_auth(),
        )
        response.raise_for_status()
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
        response_text = ""
        try:
            response = requests.patch(
                job_spec["url"],
                json={
                    "status_code": 99,
                    "status_message": f"Unclassified error {id_message}",
                },
                auth=utils.get_auth(),
            )
            response_text = response.text
            response.raise_for_status()
            joblogger.info("Reported error 99 (unclassified) to job server")
            # Don't remove remotetraceback, because we haven't considered
            # handling it explicitly, and the context could help
            joblogger.exception(error)
        except Exception as error:
            # This would most likely be an HTTP error
            joblogger.exception(error)
            joblogger.error(response_text)
    finally:
        # This shouldn't be necessary, as TemoporaryDirectories are
        # supposed to clean up after themselves on garbage
        # collection. However, getting strange errors (refusing to
        # delete non-empty subdirectories) in tempdir cleanup which
        # are hard to debug, all the more so because we currently also
        # get odd filesystem consistency errors
        try:
            subprocess.check_call(["rm", "-rf", jobrunner.tmpdir.name])
        except Exception as error:
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


def watch(queue_endpoint, loop=True, job_class=Job):
    check_environment()
    baselogger.info(f"Started watching {queue_endpoint}")
    session = requests.Session()
    # Retries for up to 2 minutes, by default
    retry = Retry(connect=30, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount(queue_endpoint, adapter)
    max_workers = os.environ.get("MAX_WORKERS", None) or max(cpu_count() - 1, 1)
    with ProcessPool(max_workers=int(max_workers)) as pool:
        while True:
            try:
                result = session.get(
                    queue_endpoint,
                    params={
                        "started": False,
                        "backend": os.environ["BACKEND"],
                        "page_size": 25,
                    },
                    auth=utils.get_auth(),
                )
                result.raise_for_status()
            except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError):
                baselogger.exception(
                    "Error when connecting to job server; sleeping for 30 seconds"
                )
                time.sleep(30)
                continue

            job_specs = result.json()
            for job_spec in job_specs["results"]:
                response = requests.patch(
                    job_spec["url"], json={"started": True}, auth=utils.get_auth()
                )
                response.raise_for_status()
                runner = job_class(job_spec)
                future = pool.schedule(runner, (), timeout=24 * HOUR,)
                future.jobrunner = runner
                future.add_done_callback(report_result)
            if loop:
                time.sleep(int(os.environ.get("POLL_INTERVAL", 5)))
            else:
                break
