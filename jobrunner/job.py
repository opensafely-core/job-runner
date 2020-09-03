import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import urllib
from pathlib import Path

from jobrunner.exceptions import DockerRunError, GitCloneError, RepoNotFound
from jobrunner.project import parse_project_yaml
from jobrunner.server_interaction import start_dependent_job_or_raise_if_unfinished
from jobrunner.utils import (
    all_output_paths_for_action,
    getlogger,
    safe_join,
    writable_job_subset,
)

logger = getlogger(__name__)


def add_github_auth_to_repo(repo):
    """Add Basic HTTP Auth to a Github repo, from the environment.

    For example, `https://github.com/sebbacon/test.git` becomes `https:/<access_token>@github.com/sebbacon/test.git`
    """
    parts = urllib.parse.urlparse(repo)
    assert not parts.username and not parts.password
    return urllib.parse.urlunparse(
        parts._replace(
            netloc=f"{os.environ['PRIVATE_REPO_ACCESS_TOKEN']}@{parts.netloc}"
        )
    )


class Job:
    def __init__(self, job_spec, workdir=None):
        if "run_locally" not in job_spec:
            job_spec["run_locally"] = False
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
        """This is necessary to satisfy `pebble`'s multiprocessing API"""
        return self.main()

    def run_job_and_dependencies(self, all_jobs=None, prepared_job=None):
        if all_jobs is None:
            all_jobs = []
        if prepared_job is None:
            prepared_job = parse_project_yaml(self.workdir, self.job_spec)
        self.logger.info(
            "Added runtime metadata to job_spec %s: %s",
            prepared_job["action_id"],
            writable_job_subset(prepared_job),
        )
        # First, run all the dependencies
        for action_id, action in prepared_job.get("dependencies", {}).items():
            if action["run_locally"]:
                self.run_job_and_dependencies(
                    all_jobs=all_jobs, prepared_job=action,
                )
            else:
                start_dependent_job_or_raise_if_unfinished(action)

        # Finally, run ourself
        if prepared_job["needs_run"]:
            self.logger.info(
                "%s needs a run; starting via docker", prepared_job["action_id"],
            )
            self.invoke_docker(prepared_job)
            prepared_job["status_message"] = "Fresh output generated"
        else:
            self.logger.info(
                "%s does not need a run; skipping docker", prepared_job["action_id"]
            )
            prepared_job["status_message"] = "Output already generated"
        all_jobs.append(prepared_job)
        return prepared_job

    def main(self):
        self.logger.info("Starting job")
        if not self.job_spec["run_locally"]:
            self.fetch_study_source()
        all_jobs = []
        self.run_job_and_dependencies(all_jobs=all_jobs)
        return all_jobs

    def __repr__(self):
        """An opaque string for use in logging to help trace events related to
        a specific job
        """
        if "url" in self.job_spec:
            match = re.match(r".*/([0-9]+)/?$", self.job_spec["url"])
            if match:
                return "job#" + match.groups()[0]
        return "-"

    def get_job_logger(self):
        return logging.LoggerAdapter(logger, {"job_id": repr(self)})

    def invoke_docker(self, prepared_job):
        # Copy expected input files into workdir
        input_files = []
        for base, relpath in prepared_job["inputs"]:
            target_path = os.path.join(self.workdir, relpath)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            shutil.copy(safe_join(base, relpath), target_path)
            input_files.append(target_path)
            self.logger.info(
                "Copied input for %s to %s", prepared_job["action_id"], target_path
            )

        cmd = [
            "docker",
            "run",
            "--name",
            prepared_job["container_name"],
            "--rm",
            "--log-driver",
            "none",
            "-a",
            "stdout",
            "-a",
            "stderr",
            "--volume",
            f"{self.workdir}:/workspace",
        ] + prepared_job["docker_invocation"]

        self.logger.info(
            "Running subdocker cmd `%s` in %s", " ".join(cmd), self.workdir
        )
        result = subprocess.run(cmd, capture_output=True, encoding="utf8")
        if result.returncode == 0:
            self.logger.info("subdocker stdout: %s", result.stdout)
        else:
            raise DockerRunError(result.stderr, report_args=False)

        # Copy expected outputs to the final location
        for base, relpath in all_output_paths_for_action(prepared_job):
            target_path = safe_join(base, relpath)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            shutil.move(safe_join(self.workdir, os.path.basename(relpath)), target_path)
            self.logger.info("Copied output to %s", target_path)

        # Delete input files
        for input_file in input_files:
            os.remove(input_file)

    def fetch_study_source(self):
        """Checkout source to a temporary location."""
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
