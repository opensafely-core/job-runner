import glob
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from jobrunner.exceptions import (
    DependencyRunning,
    DockerRunError,
    GitCloneError,
    ProjectValidationError,
    RepoNotFound,
)
from jobrunner.project import RUN_COMMANDS_CONFIG, parse_project_yaml
from jobrunner.server_interaction import start_dependent_job_or_raise_if_unfinished
from jobrunner.utils import getlogger, safe_join, writable_job_subset

logger = getlogger(__name__)


def fix_ownership(path):
    """Recursively change ownership of all files at the given location to the current user.

    In production, where everything is run in docker, the effective user is always root. However, when testing from the command line, this is not necessarily the case
    """
    # Abritrarily, we pick a known docker image which already runs as root, and
    # has bash and chown installed
    image = RUN_COMMANDS_CONFIG["cohortextractor"]["docker_invocation"][0]
    mounted_path = Path("/tmp") / Path(path).relative_to("/")
    # Run the docker command
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
        "--volume",
        f"{path}:{mounted_path}",
        "--entrypoint",
        "/bin/bash",
        image,
        "-c",
        f'"chown -R  {os.getuid()} {mounted_path}"',
    ]
    cmd = f"docker run --rm --log-driver none -a stdout -a stderr --volume {path}:{mounted_path} --entrypoint /bin/bash {image} -c 'chown -R  1000 {mounted_path}'"
    result = subprocess.run(cmd, capture_output=True, encoding="utf8", shell=True)
    if result.returncode != 0:
        raise DockerRunError(result.stderr, report_args=False)


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
        last_error = None
        for action_id, action in prepared_job.get("dependencies", {}).items():
            if action["run_locally"]:
                self.run_job_and_dependencies(
                    all_jobs=all_jobs, prepared_job=action,
                )
            else:
                # Don't exit on the first failure: attempt to run every dependency
                try:
                    start_dependent_job_or_raise_if_unfinished(action)
                except DependencyRunning as e:
                    last_error = e
        if last_error:
            raise last_error

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
        self.logger.info("Job and all its dependencies finished")
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
        """Copy required inputs into place from persistent storage; run a docker
        container; and copy its outputs back into persistent storage
        """
        # An output is stored on the filesystem at a location defined by joining
        # (base_path, namespace, relative_path). The base_path is typically a
        # volume permissioned specifically for a given privacy level; the
        # namespace is derived from the `outputs` keys in `project.yaml` and
        # ensures different actions with identical filenames don't clash.  The
        # relative_path is a path to a file, possibly in subfolders, relative to
        # a directory decided at runtime. This directory will be either the
        # namespaced base path (when we are retrieving or saving files in
        # persistent storage), or a temporary working folder (for scripts
        # running via docker).

        # Copy expected input files into workdir, expanding shell globs
        self.logger.debug(
            "Mapping %s readonly inputs to %s", prepared_job["inputs"], self.workdir
        )
        input_volumes = []
        seen_relpaths = []
        for location in prepared_job["inputs"]:
            namespace_path = safe_join(location["base_path"], location["namespace"])
            source_paths = glob.glob(
                safe_join(namespace_path, location["relative_path"])
            )
            for source_path in source_paths:
                relpath = os.path.relpath(source_path, start=namespace_path)
                if relpath in seen_relpaths:
                    raise ProjectValidationError(
                        f"Found duplicate input file {relpath}", report_args=True
                    )
                seen_relpaths.append(relpath)
                input_volumes.extend(
                    ["--volume", f"{source_path}:/workspace/{relpath}:ro"]
                )
        # Run the docker command
        cmd = (
            [
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
            ]
            + input_volumes
            + prepared_job["docker_invocation"]
        )
        self.logger.info(
            "Running subdocker cmd `%s` in %s", " ".join(cmd), self.workdir
        )
        result = subprocess.run(cmd, capture_output=True, encoding="utf8")
        if result.returncode == 0:
            self.logger.info("subdocker stdout: %s", result.stdout)
        else:
            raise DockerRunError(result.stderr, report_args=False)

        # Copy expected outputs to the final location
        fix_ownership(self.workdir)
        for location in prepared_job["output_locations"]:
            source_path_pattern = safe_join(self.workdir, location["relative_path"])
            self.logger.debug(
                "Looking for outputs to copy to storage at %s", source_path_pattern
            )
            found_any = False
            for source_path in glob.glob(source_path_pattern):
                found_any = True
                relpath = os.path.join(
                    location["namespace"],
                    os.path.relpath(source_path, start=self.workdir),
                )
                target_path = safe_join(location["base_path"], relpath)
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                subprocess.check_call(["mv", source_path, target_path])
                self.logger.info("Copied output to %s", target_path)
            if not found_any:
                raise DockerRunError(
                    f"No expected outputs found at {source_path_pattern}",
                    report_args=True,
                )
        return prepared_job

    def fetch_study_source(self):
        """Checkout source to a temporary location."""
        repo = self.job_spec["workspace"]["repo"]
        branch = self.job_spec["workspace"]["branch"]
        max_retries = 5
        sleep = 4
        os.makedirs(self.workdir, exist_ok=True)
        os.chdir(self.workdir)
        subprocess.check_call(["git", "init"])
        for attempt in range(max_retries + 1):
            os.makedirs(self.workdir, exist_ok=True)
            os.chdir(self.workdir)
            try:
                subprocess.check_call(["git", "init"])
                cmd = [
                    "git",
                    "pull",
                    "--depth",
                    "1",
                    repo,
                    branch,
                ]
                self.logger.info("Running %s, attempt %s", " ".join(cmd), attempt)
                subprocess.check_output(
                    cmd,
                    stderr=subprocess.STDOUT,
                    encoding="utf8",
                    env=dict(
                        os.environ,
                        # This script will supply the access token from the
                        # environment variable PRIVATE_REPO_ACCESS_TOKEN
                        GIT_ASKPASS=os.path.join(
                            os.path.dirname(__file__), "git_askpass_access_token.py"
                        ),
                    ),
                )
                break
            except subprocess.CalledProcessError as e:
                if e.output and "not found" in e.output:
                    raise RepoNotFound(e.output, report_args=True)
                elif attempt < max_retries:
                    self.logger.warning(
                        "Failed clone to %s (message `%s`); sleeping %s, then retrying",
                        self.workdir,
                        e.output,
                        sleep,
                    )
                    shutil.rmtree(self.workdir, ignore_errors=True)
                    time.sleep(sleep)
                    sleep *= 2
                else:
                    raise GitCloneError(" ".join(cmd), report_args=True) from e
