from pathlib import Path
from urllib.parse import urlparse
from pebble import ProcessPool
from concurrent.futures import TimeoutError
import logging
import os
import requests
import subprocess
import tempfile
import time

HOUR = 60 * 60
POLL_INTERVAL = 1

logging.basicConfig(
    level=logging.INFO,
    filename="app.log",
    filemode="w",
    format="%(name)s - %(levelname)s - %(message)s",
)


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
        if mimetype not in ["text", "inode"]:
            raise RuntimeError(
                f"All analysis input files must be text, found {mimetype} at {path}"
            )


def notify_completion():
    """Mark job as complete in job queue
    """
    pass


def run_cohort_extractor(workdir, volume_name):
    storage_base = Path(os.environ["OPENSAFELY_RUNNER_STORAGE_BASE"])
    output_path = storage_base / volume_name
    output_path.mkdir(parents=True, exist_ok=True)
    database_url = os.environ["DATABASE_URL"]
    cmd = [
        "docker",
        "run",
        "--rm",
        "--log-driver",
        "none",
        "-a",
        "stdout",
        "-a",
        "stderr",
        "--mount",
        f"source={output_path},dst=/workspace/output,type=bind",
        "--mount",
        f"source={workdir}/analysis,dst=/workspace/analysis,type=bind",
        "docker.pkg.github.com/ebmdatalab/opensafely-research-template/cohort-extractor",
        "generate_cohort",
        f"--database-url={database_url}",
    ]
    os.chdir(workdir)
    stdout_log_path = storage_base / volume_name / "stdout.log"
    stderr_log_path = storage_base / volume_name / "stderr.log"
    with open(stdout_log_path, "w") as stdout_log, open(
        stderr_log_path, "w"
    ) as stderr_log:

        result = subprocess.run(cmd, check=True, capture_output=True, encoding="utf8")
        stdout_log.write(result.stdout)
        stderr_log.write(result.stderr)
    return output_path


def make_volume_name(repo, branch_or_tag):
    repo_name = urlparse(repo).path[1:].split("/")[-1]
    return repo_name + "-" + branch_or_tag


def fetch_study_source(
    repo, branch_or_tag, workdir,
):
    """Checkout source over Github API to a temporary location.
    """
    cmd = ["git", "clone", "--depth", "1", "--branch", branch_or_tag, repo, workdir]
    subprocess.run(cmd, check=True)


def report_result(future):
    job = future.job
    try:
        job = future.result()  # blocks until results are ready
        requests.patch(
            job["url"],
            json={"status_code": 0, "output_url": job["output_url"]},
            auth=get_auth(),
        )
    except TimeoutError as error:
        requests.patch(job["url"], json={"status_code": -1}, auth=get_auth())
        logging.exception(error)
    except Exception as error:
        requests.patch(job["url"], json={"status_code": 1}, auth=get_auth())
        logging.exception(error)


def run_job(job):
    repo = job["repo"]
    tag = job["tag"]
    logging.info(f"Starting job {job}")
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)
        volume_name = make_volume_name(repo, tag)
        workdir = os.path.join(tmpdir, volume_name)
        fetch_study_source(repo, tag, workdir)
        validate_input_files(workdir)
        job["output_url"] = str(run_cohort_extractor(workdir, volume_name))
        return job


def get_auth():
    return (os.environ["QUEUE_USER"], os.environ["QUEUE_PASS"])


def watch(queue_endpoint, loop=True):
    with ProcessPool(max_tasks=50) as pool:
        while True:
            jobs = requests.get(
                queue_endpoint, params={"started": False, "page_size": 100}
            ).json()
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
