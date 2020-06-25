from pathlib import Path
from urllib.parse import urlparse
from pebble import ProcessPool
from concurrent.futures import TimeoutError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging
import os
import re
import requests
import subprocess
import sys
import tempfile
import time

from tinynetrc import Netrc


HOUR = 60 * 60
POLL_INTERVAL = 1

logging.basicConfig(level=logging.INFO, stream=sys.stdout)


def validate_input_files(workdir):
    """Assert that all the input files are text, not binary
    """
    workdir = Path(workdir)
    missing = []
    for required in ["analysis", "codelists"]:
        if not (workdir / required).exists():
            missing.append(required)
    if missing:
        raise RuntimeError(
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
            raise RuntimeError(
                f"All analysis input files must be text, found {result} at {path}"
            )
    logging.info(f"Repo at {workdir} successfully validated")


def run_cohort_extractor(database_url, workdir, volume_name):
    # If running this within a docker container, the storage base
    # should be a volume mounted from the docker host
    storage_base = Path(os.environ["OPENSAFELY_RUNNER_STORAGE_BASE"])
    # We create `output_path` and then map it straight through to the
    # inner docker container, so that docker-within-docker can write
    # straight through to the (optionally-mounted) storage base
    output_path = storage_base / volume_name
    output_path.mkdir(parents=True, exist_ok=True)
    # By setting the name to the volume_name, we are guaranteeing only
    # one identical job can run at once
    container_name = re.sub(r"[^a-zA-Z0-9]", "-", volume_name)
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
    logging.info(f"Running subdocker cmd `{' '.join(cmd)}`")
    result = subprocess.run(cmd, capture_output=True, encoding="utf8")
    if result.returncode == 0:
        log = logging.info
    else:
        log = logging.error
    log(f"cohort-extractor subdocker stdout: {result.stdout}")
    log(f"cohort-extractor subdocker stderr: {result.stderr}")
    result.check_returncode()
    return output_path


def make_volume_name(repo, branch_or_tag):
    repo_name = urlparse(repo).path[1:].split("/")[-1]
    return repo_name + "-" + branch_or_tag


def fetch_study_source(
    repo, branch_or_tag, workdir,
):
    """Checkout source over Github API to a temporary location.
    """
    max_retries = 3
    for attempt in range(max_retries + 1):
        cmd = ["git", "clone", "--depth", "1", "--branch", branch_or_tag, repo, workdir]
        msg = f"Running `{' '.join(cmd)}`"
        if attempt > 0:
            msg += f" (attempt #{attempt})"
        logging.info(msg)
        try:
            subprocess.check_output(cmd, stderr=subprocess.STDOUT, encoding="utf8")
            break
        except subprocess.CalledProcessError as e:
            if "Repository not found" in e.output:
                raise
            elif attempt < max_retries:
                logging.warning(f"Failed `{' '.join(cmd)}`; sleeping, then retrying")
                time.sleep(10)
            else:
                raise


def report_result(future):
    job = future.job
    try:
        job = future.result()  # blocks until results are ready
        requests.patch(
            job["url"],
            json={"status_code": 0, "output_url": job["output_url"]},
            auth=get_auth(),
        )
        logging.info(f"Reported success for job {job}")
    except TimeoutError as error:
        requests.patch(job["url"], json={"status_code": -1}, auth=get_auth())
        logging.exception(error)
    except Exception as error:
        requests.patch(job["url"], json={"status_code": 1}, auth=get_auth())
        logging.exception(error)


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


def run_job(job):
    repo = job["repo"]
    tag = job["tag"]
    db = job["db"]
    set_auth()
    database_url = os.environ[f"{db.upper()}_DATABASE_URL"]
    logging.info(f"Starting job {job}")
    with tempfile.TemporaryDirectory(
        dir=os.environ["OPENSAFELY_RUNNER_STORAGE_BASE"]
    ) as tmpdir:
        os.chdir(tmpdir)
        volume_name = make_volume_name(repo, tag)
        workdir = os.path.join(tmpdir, volume_name)
        fetch_study_source(repo, tag, workdir)
        validate_input_files(workdir)
        job["output_url"] = str(
            run_cohort_extractor(database_url, workdir, volume_name)
        )
        return job


def get_auth():
    return (os.environ["QUEUE_USER"], os.environ["QUEUE_PASS"])


def watch(queue_endpoint, loop=True):
    logging.info(f"Started watching {queue_endpoint}")
    session = requests.Session()
    # Retries for up to 2 minutes, by default
    retry = Retry(connect=30, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount(queue_endpoint, adapter)
    with ProcessPool(max_tasks=50) as pool:
        while True:
            logging.debug(f"Polling {queue_endpoint}")
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
                logging.exception("Connection error; sleeping for 15 mins")
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
                future = pool.schedule(run_job, (job,), timeout=6 * HOUR,)
                future.job = job
                future.add_done_callback(report_result)
            if loop:
                time.sleep(POLL_INTERVAL)
            else:
                break
