import glob
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import PosixPath

from jobrunner import utils
from jobrunner.exceptions import (
    DependencyRunning,
    DockerRunError,
    GitCloneError,
    ProjectValidationError,
    RepoNotFound,
)
from jobrunner.project import parse_project_yaml
from jobrunner.server_interaction import start_dependent_job_or_raise_if_unfinished

logger = utils.getlogger(__name__)


def get_input_filespec_from_job(prepared_job):
    """Create an `input_file_spec` for supplying as an argument
    `volume_from_filespec`.

    `input_file_spec` is a list of `(source_path, relpath)` tuples.

    Args:

        prepared_job: a finalised "job spec" dict

    Raises:

        ProjectValidationError: if duplicate relpaths are defined in the project
    """
    # Create a list of (source, destination) tuples that correspond with
    # required inputs. Also, raise an exception if duplicate inputs are found
    # (this is very unlikely, but possible, because the filesystem is not
    # under our exclusive control)
    inputs = []
    seen_relpaths = []

    for location in prepared_job["inputs"]:
        namespace_path = utils.safe_join(location["base_path"], location["namespace"])
        source_paths = glob.glob(
            utils.safe_join(namespace_path, location["relative_path"])
        )
        for source_path in source_paths:
            relpath = os.path.relpath(source_path, start=namespace_path)
            if relpath in seen_relpaths:
                raise ProjectValidationError(
                    f"Found duplicate input file {relpath}", report_args=True
                )
            seen_relpaths.append(relpath)
            inputs.append((source_path, relpath))
    return inputs


@contextmanager
def volume_from_filespec(input_file_spec):
    """Create a docker volume, and copy the contents of wordir, and the supplied
    input files, into `/workspace`.  When the contextmanager exits, remove the
    volume and the container that accesses it.

    Args:

        input_file_spec: a list of (absolute_src_path, relative_dst_path)
        tuples to copy into the volume

    Returns:

         tuple: the name of the volume created, and the name of a running container that can be used for copying to/from the volume
    """

    volume_name = subprocess.check_output(
        ["docker", "volume", "create"], encoding="utf8"
    ).strip()
    volume_container_name = f"volume-maker-{volume_name}"
    # Create a temporary container the exclusive purpose of copying data onto a
    # new volume. We use the job-runner image for convenience, but it could be
    # any image with `cp` and `mkdir` available. Because we keep the TTY open
    # the container continues to run when daemonised
    cmd = [
        "docker",
        "run",
        "--entrypoint",
        "bash",
        "-t",
        "--rm",
        "-d",
        "--name",
        volume_container_name,
        "-v",
        f"{volume_name}:{utils.get_workdir()}",
        "docker.opensafely.org/job-runner",
    ]
    subprocess.check_call(cmd, encoding="utf8")

    # Copy data to the root of the volume, creating directory structures as we
    # go
    for source_path, relpath in input_file_spec:
        cmd = [
            "docker",
            "exec",
            volume_container_name,
            "mkdir",
            "-p",
            utils.safe_join(utils.get_workdir(), os.path.dirname(relpath)),
        ]
        subprocess.check_call(cmd)
        cmd = [
            "docker",
            "cp",
            source_path,
            f"{volume_container_name}:{utils.safe_join(utils.get_workdir(), relpath)}",
        ]
        subprocess.check_call(cmd)

    try:
        yield volume_name, volume_container_name
    finally:
        cmd = ["docker", "stop", volume_container_name]
        subprocess.check_call(cmd)
        # docker volumes are reference counted, apparently
        # non-deterministically, so wait to ensure that the docker daemon has
        # decremented the volume count, or we can not remove it.
        time.sleep(0.1)
        cmd = ["docker", "volume", "rm", volume_name]
        subprocess.check_call(cmd)


def copy_from_container(container_name, file_copy_spec):
    """Copy the specified files from the container, preserving the part of their
    path relative to a base location.

    Args:

        container_name: the name of a running docker container
        file_copy_spec: a list of triples of the form `(source_base, dest_base, rel_path_with_glob)`.

    Returns:

        None

    Example:

        Given a container named `arbitrary_fox`, with files at `/workspace/foo/bar1.txt` and `/workspace/foo/bar2.txt`, the following will copy them to the docker host at `/mnt/backups/foo/bar1.txt` and  `/mnt/backups/foo/bar2.txt`:

            file_copy_spec = [("/workspace", "/mnt/backups", "foo/bar*.txt")]
            copy_from_container("arbitrary_fox", file_copy_spec)`


    """
    for source_base, dest_base, rel_path_with_glob in file_copy_spec:
        found_any = False
        source_path_with_glob = utils.safe_join(source_base, rel_path_with_glob)
        source_paths = subprocess.check_output(
            [
                "docker",
                "exec",
                container_name,
                "find",
                "/",
                "-path",
                source_path_with_glob,
            ],
            encoding="utf8",
        ).splitlines()
        for source_path in source_paths:
            found_any = True
            rel_path = os.path.relpath(source_path, source_base)
            dest_path = utils.safe_join(dest_base, rel_path)
            dest_dir = os.path.dirname(dest_path)
            os.makedirs(dest_dir, exist_ok=True)
            cmd = [
                "docker",
                "cp",
                f"{container_name}:{source_path}",
                dest_path,
            ]
            subprocess.check_call(cmd)
        if not found_any:
            raise DockerRunError(
                f"No expected outputs found at {source_path_with_glob}",
                report_args=True,
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
            self.workdir = PosixPath(self.tmpdir.name)
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
            utils.writable_job_subset(prepared_job),
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

        # Copy expected input files into workdir, expanding shell globs
        self.logger.debug(
            "Copying %s inputs to %s", prepared_job["inputs"], self.workdir
        )
        # This initial filespec entry will copy the study repo to the root. It's
        # important this happens before input files are copied in, so it doesn't
        # overwrite any important data
        input_filespec = [(str(self.workdir) + "/.", ".")]
        # Now add all the other inputs from the job
        input_filespec.extend(get_input_filespec_from_job(prepared_job))
        with volume_from_filespec(input_filespec) as volume_info:
            volume_name, volume_container_name = volume_info
            try:
                run_cmd = [
                    "docker",
                    "run",
                    "--rm",
                    "--init",
                    "--name",
                    prepared_job["container_name"],
                    "--volume",
                    f"{volume_name}:{utils.get_workdir()}",
                ]
                # Run the docker command
                cmd = run_cmd + prepared_job["docker_invocation"]
                self.logger.info(
                    "Running subdocker cmd `%s` in %s", " ".join(cmd), self.workdir
                )
                subprocess.run(cmd, check=True, capture_output=True, encoding="utf8")

                # Copy expected outputs to the final location
                file_copy_triples = []
                for location in prepared_job["output_locations"]:
                    # safe_join
                    dest_base = os.path.join(
                        location["base_path"], location["namespace"]
                    )
                    file_copy_triples.append(
                        (utils.get_workdir(), dest_base, location["relative_path"])
                    )
                self.logger.debug("Copying %s output specs", len(file_copy_triples))
                copy_from_container(volume_container_name, file_copy_triples)
            except subprocess.CalledProcessError as e:
                raise DockerRunError(e.stderr, report_args=False)

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
