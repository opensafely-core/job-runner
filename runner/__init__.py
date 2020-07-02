from concurrent.futures import TimeoutError
from pathlib import Path
from pebble import ProcessPool
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from urllib.parse import urlparse
import logging
import os
import re
import requests
import subprocess
import tempfile
import time

from tinynetrc import Netrc

from runner.exceptions import CohortExtractorError
from runner.exceptions import GitCloneError
from runner.exceptions import InvalidRepo
from runner.exceptions import OpenSafelyError
from runner.exceptions import RepoNotFound

HOUR = 60 * 60
POLL_INTERVAL = 1
COHORT_EXTRACTOR_TIMEOUT = 24 * HOUR

# Create a logger with a field for recording a unique job id, and a
# `baselogger` adapter which fills this field with a hyphen, for use
# when logging events not associated with jobs
FORMAT = "%(asctime)-15s %(levelname)-10s  %(job_id)-10s %(message)s"
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(FORMAT)
handler.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(handler)
baselogger = logging.LoggerAdapter(logger, {"job_id": "-"})


def job_id_from_job(job):
    """An opaque string for use in logging to help trace events related to
    a specific job
    """
    return "job#" + re.match(r".*/([0-9]+)/?$", job["url"]).groups()[0]


def get_job_logger(job):
    job_id = job_id_from_job(job)
    return logging.LoggerAdapter(logger, {"job_id": job_id})


def validate_input_files(workdir):
    """Assert that all the input files are text, not binary
    """
    workdir = Path(workdir)
    missing = []
    for required in ["analysis", "codelists"]:
        if not (workdir / required).exists():
            missing.append(required)
    if missing:
        raise InvalidRepo(
            f"Folders {', '.join(missing)} must exist; is this an OpenSAFELY repo?"
        )
    for path in workdir.rglob("*"):
        path = str(path)
        if ".git" in path or "outputs" in path:
            continue
        # We shell out to system's libmagic implementation, rather than
        # using python, to reduce dependencies
        result = subprocess.check_output(
            ["file", "--brief", "--mime", path], encoding="utf8"
        )
        mimetype = result.split("/")[0]
        if mimetype not in ["text", "inode"] and not result.startswith(
            "application/pdf"
        ):
            raise InvalidRepo(
                f"All analysis input files must be text, found {result} at {path}"
            )


def run_cohort_extractor(job):
    joblogger = get_job_logger(job)
    joblogger.info(f"Starting job")
    repo = job["repo"]
    tag = job["tag"]
    db = job["db"]
    set_auth()
    database_url = os.environ[f"{db.upper()}_DATABASE_URL"]
    with tempfile.TemporaryDirectory(
        dir=os.environ["OPENSAFELY_RUNNER_STORAGE_BASE"]
    ) as tmpdir:
        os.chdir(tmpdir)
        volume_name = make_volume_name(repo, tag, db)
        container_name = make_container_name(volume_name)
        workdir = os.path.join(tmpdir, volume_name)
        fetch_study_source(workdir, job)
        validate_input_files(workdir)
        joblogger.info(f"Repo at {workdir} successfully validated")

        # If running this within a docker container, the storage base
        # should be a volume mounted from the docker host
        storage_base = Path(os.environ["OPENSAFELY_RUNNER_STORAGE_BASE"])
        # We create `output_path` and then map it straight through to the
        # inner docker container, so that docker-within-docker can write
        # straight through to the (optionally-mounted) storage base
        output_path = storage_base / volume_name
        output_path.mkdir(parents=True, exist_ok=True)
        cmd = [
            "docker",
            "run",
            "--name",
            container_name,
            "--rm",
            "--log-driver",
            "none",
            "-a",
            "stdout",
            "-a",
            "stderr",
            "--volume",
            f"{output_path}:{output_path}",
            "--volume",
            f"{workdir}/analysis:/workspace/analysis",
            "--volume",
            f"{workdir}/codelists:/workspace/codelists",
            "docker.pkg.github.com/opensafely/cohort-extractor/cohort-extractor:latest",
            "generate_cohort",
            f"--database-url={database_url}",
            f"--output-dir={output_path}",
        ]

        os.chdir(workdir)
        joblogger.info("Running subdocker cmd `%s` in %s", cmd, workdir)
        result = subprocess.run(cmd, capture_output=True, encoding="utf8")
        if result.returncode == 0:
            joblogger.info("cohort-extractor subdocker stdout: %s", result.stdout)
        else:
            raise CohortExtractorError(result.stderr)
        job["output_url"] = str(output_path)
        return job


def make_volume_name(repo, branch_or_tag, db_flavour):
    repo_name = urlparse(repo).path[1:]
    if repo_name.endswith("/"):
        repo_name = repo_name[:-1]
    repo_name = repo_name.split("/")[-1]
    return repo_name + "-" + branch_or_tag + "-" + db_flavour


def make_container_name(volume_name):
    # By basing the container name to the volume_name, we are
    # guaranteeing only one identical job can run at once by docker
    container_name = re.sub(r"[^a-zA-Z0-9]", "-", volume_name)
    # Remove any leading dashes, as docker requires images begin with [:alnum:]
    if container_name.startswith("-"):
        container_name = container_name[1:]
    return container_name


def fetch_study_source(workdir, job):
    """Checkout source over Github API to a temporary location.
    """
    repo = job["repo"]
    branch_or_tag = job["tag"]
    max_retries = 3
    joblogger = get_job_logger(job)
    for attempt in range(max_retries + 1):
        cmd = ["git", "clone", "--depth", "1", "--branch", branch_or_tag, repo, workdir]
        joblogger.info("Running %s, attempt %s", cmd, attempt)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, encoding="utf8")
            break
        except subprocess.CalledProcessError as e:
            if "not found" in e.output:
                raise RepoNotFound(e.output)
            elif attempt < max_retries:
                joblogger.warning("Failed clone; sleeping, then retrying")
                time.sleep(10)
            else:
                raise GitCloneError(cmd) from e


def report_result(future):
    job = future.job
    joblogger = get_job_logger(job)
    id_message = f"id {job_id_from_job(job)}"
    try:
        job = future.result(
            timeout=COHORT_EXTRACTOR_TIMEOUT
        )  # blocks until results are ready
        requests.patch(
            job["url"],
            json={"status_code": 0, "output_url": job["output_url"]},
            auth=get_auth(),
        )
        joblogger.info("Reported success to job server")
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
            "Reported error %s (%s) to job server",
            error.status_code,
            f"{repr(error)} {id_message}",
        )
        # Remove pebble's RemoteTraceback exception from reporting
        error.__cause__ = None
        joblogger.exception(error)
    except Exception as error:
        requests.patch(
            job["url"],
            json={
                "status_code": 99,
                "status_message": "Unclassified error {id_message}",
            },
            auth=get_auth(),
        )
        joblogger.info("Reported error 99 (unclassified) to job server")
        # Don't remove remotetraceback, because we haven't considered
        # handling it explicitly, and the context could help
        joblogger.exception(error)


def set_auth():
    """Set HTTP auth (used by `requests`)
    """
    netrc_path = os.path.join(os.path.expanduser("~"), ".netrc")
    if not os.path.exists(netrc_path):
        with open(netrc_path, "w") as f:
            f.write("")
    netrc = Netrc()
    if netrc["github.com"]["password"]:
        login = netrc["github.com"]["login"]
        password = netrc["github.com"]["password"]
    else:
        password = os.environ["PRIVATE_REPO_ACCESS_TOKEN"]
        login = "doesntmatter"
        netrc["github.com"] = {
            "login": login,
            "password": password,
        }
        netrc.save()
    return (login, password)


def get_auth():
    return (os.environ["QUEUE_USER"], os.environ["QUEUE_PASS"])


def watch(queue_endpoint, loop=True):
    baselogger.info(f"Started watching {queue_endpoint}")
    session = requests.Session()
    # Retries for up to 2 minutes, by default
    retry = Retry(connect=30, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount(queue_endpoint, adapter)
    with ProcessPool(max_tasks=50) as pool:
        while True:
            baselogger.debug(f"Polling {queue_endpoint}")
            try:
                result = session.get(
                    queue_endpoint,
                    params={
                        "started": False,
                        "backend": os.environ["BACKEND"],
                        "page_size": 100,
                    },
                    auth=get_auth(),
                )
            except requests.exceptions.ConnectionError:
                baselogger.exception("Connection error; sleeping for 15 mins")
                time.sleep(60 * 15)
            result.raise_for_status()
            jobs = result.json()
            for job in jobs["results"]:
                assert (
                    job["operation"] == "generate_cohort"
                ), f"The only currently-supported operation is `generate_cohort`, not `{job['operation']}`"
                response = requests.patch(
                    job["url"], json={"started": True}, auth=get_auth()
                )
                response.raise_for_status()
                future = pool.schedule(run_cohort_extractor, (job,), timeout=6 * HOUR,)
                future.job = job
                future.add_done_callback(report_result)
            if loop:
                time.sleep(POLL_INTERVAL)
            else:
                break
