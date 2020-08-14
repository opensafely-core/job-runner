import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import urllib
from pathlib import Path

from runner.exceptions import DockerRunError, GitCloneError, RepoNotFound
from runner.project import parse_project_yaml
from runner.server_interaction import start_dependent_job_or_raise_if_unfinished
from runner.utils import all_output_paths_for_action, getlogger, needs_run

logger = getlogger(__name__)


def add_github_auth_to_repo(repo):
    parts = urllib.parse.urlparse(repo)
    assert not parts.username and not parts.password
    return urllib.parse.urlunparse(
        parts._replace(
            netloc=f"{os.environ['PRIVATE_REPO_ACCESS_TOKEN']}@{parts.netloc}"
        )
    )


class Job:
    def __init__(self, job_spec, workdir=None):
        self.job_spec = job_spec
        self.tmpdir = tempfile.TemporaryDirectory(
            dir=os.environ["HIGH_PRIVACY_STORAGE_BASE"]
        )
        if workdir is None:
            self.workdir = Path(self.tmpdir.name)
        else:
            self.workdir = workdir
        self.logger = self.get_job_logger()

    def __call__(self):
        """This is necessary to satisfy `pebble`'s multiprocessing API
        """
        return self.main()

    def run_or_enqueue_job_and_dependencies(self):
        for action_id, action in self.prepared_job["dependencies"].items():
            start_dependent_job_or_raise_if_unfinished(action)
        if needs_run(self.prepared_job):
            self.invoke_docker()
            self.prepared_job["status_message"] = "Fresh output generated"
        else:
            self.prepared_job["status_message"] = "Output already generated"

    def main(self):
        self.logger.info("Starting job")
        self.fetch_study_source()
        self.logger.info(f"Repo at {self.workdir} successfully validated")
        self.prepared_job = parse_project_yaml(self.workdir, self.job_spec)
        self.logger.debug(f"Added runtime metadata to job_spec: {self.prepared_job}")
        self.run_or_enqueue_job_and_dependencies()
        return self.prepared_job

    def __repr__(self):
        """An opaque string for use in logging to help trace events related to
        a specific job
        """
        match = re.match(r".*/([0-9]+)/?$", self.job_spec["url"])
        if match:
            return "job#" + match.groups()[0]
        else:
            return "-"

    def get_job_logger(self):
        return logging.LoggerAdapter(logger, {"job_id": repr(self)})

    def invoke_docker(self):
        # Copy expected input files into workdir
        for input_name, input_path in self.prepared_job.get(
            "namespaced_inputs", []
        ).items():
            target_path = os.path.join(self.workdir, input_name)
            shutil.move(input_path, target_path)
            self.logger.info("Copied input to %s", target_path)

        cmd = [
            "docker",
            "run",
            "--name",
            self.prepared_job["container_name"],
            "--rm",
            "--log-driver",
            "none",
            "-a",
            "stdout",
            "-a",
            "stderr",
            "--volume",
            f"{self.workdir}:/workspace",
        ] + self.prepared_job["docker_invocation"]

        self.logger.info("Running subdocker cmd `%s` in %s", cmd, self.workdir)
        result = subprocess.run(cmd, capture_output=True, encoding="utf8")
        if result.returncode == 0:
            self.logger.info("subdocker stdout: %s", result.stdout)
        else:
            raise DockerRunError(result.stderr, report_args=False)

        # Copy expected outputs to the appropriate location
        for _, _, target_path in all_output_paths_for_action(self.prepared_job):
            filename = os.path.basename(target_path)
            shutil.move(os.path.join(self.workdir, filename), target_path)
            self.logger.info("Copied output to %s", target_path)

    def fetch_study_source(self):
        """Checkout source to a temporary location.
        """
        repo = self.job_spec["workspace"]["repo"]
        branch = self.job_spec["workspace"]["branch"]
        max_retries = 3
        # We use URL-based authentication to access private repos
        # (q.v. `add_github_auth_to_repo`, above).
        #
        # Because `git clone` causes these URLs to be written to disk
        # (in `~/.git/config`), we instead use `git pull`, which
        # requires a folder to be initialised as a git repo
        os.makedirs(self.workdir, exist_ok=True)
        os.chdir(self.workdir)
        subprocess.check_call(["git", "init"])
        for attempt in range(max_retries + 1):
            # We attempt this 3 times, to assuage any network / github
            # flakiness
            cmd = [
                "git",
                "pull",
                "--depth",
                "1",
                add_github_auth_to_repo(repo),
                branch,
            ]
            loggable_cmd = (
                " ".join(cmd).replace(
                    os.environ["PRIVATE_REPO_ACCESS_TOKEN"], "xxxxxxxxx"
                ),
            )
            self.logger.info("Running %s, attempt %s", loggable_cmd, attempt)
            try:
                subprocess.check_output(cmd, stderr=subprocess.STDOUT, encoding="utf8")
                break
            except subprocess.CalledProcessError as e:
                if "not found" in e.output:
                    raise RepoNotFound(e.output, report_args=True)
                elif attempt < max_retries:
                    self.logger.warning("Failed clone; sleeping, then retrying")
                    time.sleep(10)
                else:
                    raise GitCloneError(cmd, report_args=True) from e
