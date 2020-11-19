"""
NOTE: This module exists purely as a temporary shim to fake enough of the old
job-runner API to keep the cohortextractor integration working unchanged.
"""
import os
from pathlib import Path
import random
import shutil
import string
import subprocess

import jobrunner.run
from . import config
from . import docker
from .database import select_values
from .manage_jobs import METADATA_DIR
from .models import JobRequest, Job as DatabaseJob
from .create_or_update_jobs import create_jobs
from .log_utils import configure_logging


class Job:
    def __init__(self, job_spec, workdir):
        if (
            job_spec["backend"] != "expectations"
            or job_spec["workspace"]["db"] != "dummy"
        ):
            raise RuntimeError(
                "This command can only be used with the 'expectations' "
                "backend and the 'dummy' database"
            )
        self.requested_actions = [job_spec["action_id"]]
        self.force_run_dependencies = job_spec["force_run_dependencies"]
        self.project_dir = Path(workdir)
        pass

    def main(self):
        # setup config
        config.LOCAL_RUN_MODE = True
        config.WORK_DIR = self.project_dir / METADATA_DIR / ".internal"
        config.DATABASE_FILE = config.WORK_DIR / "db.sqlite"
        config.JOB_LOG_DIR = config.WORK_DIR / "logs"
        config.BACKEND = "expectations"
        config.USING_DUMMY_DATA_BACKEND = True
        # Generate unique docker label to use for all volumes and containers to
        # make cleanup easy
        docker_label = "job-runner-local-{}".format(
            "".join(random.choices(string.ascii_uppercase, k=8))
        )
        docker.LABEL = docker_label
        # None of the below should be used when running locally
        config.TMP_DIR = None
        config.GIT_REPO_DIR = None
        config.HIGH_PRIVACY_STORAGE_BASE = None
        config.MEDIUM_PRIVACY_STORAGE_BASE = None
        config.HIGH_PRIVACY_WORKSPACES_DIR = None
        config.MEDIUM_PRIVACY_WORKSPACES_DIR = None

        # create job_request
        job_request = JobRequest(
            id="local",
            repo_url=str(self.project_dir),
            commit="none",
            requested_actions=self.requested_actions,
            workspace="local",
            database_name="dummy",
            force_run_dependencies=self.force_run_dependencies,
            branch="",
            original={"created_by": os.environ.get("USERNAME")},
        )
        create_jobs(job_request)
        actions = select_values(DatabaseJob, "action")
        print(f"\nRunning actions: {', '.join(actions)}\n")
        configure_logging(show_action_name_only=True)
        try:
            jobrunner.run.main(exit_when_done=True)
        except:
            print("\nCleaning up Docker containers and volumes ...")
            raise
        finally:
            delete_docker_entities("container", docker_label)
            delete_docker_entities("volume", docker_label)
            shutil.rmtree(config.WORK_DIR)
        # run main loop
        # if SIGINT, kill all jobs keep running main loop
        # print some useful output
        # remove `.internal` directory

        # Needs to return a result which satisfies this code
        #
        #    if result:
        #        print("Generated outputs:")
        #        output = PrettyTable()
        #        output.field_names = ["status", "path"]

        #        for action in result:
        #            for location in action["output_locations"]:
        #                output.add_row(
        #                    [
        #                        action["status_message"],
        #                        location["relative_path"],
        #                    ]
        #                )
        #        print(output)
        #    else:
        #        print("Nothing to do")

    # We need to support `job.logger.setLevel()` and this is the easiest way to
    # do this
    @property
    def logger(self):
        return self

    def setLevel(self, log_level):
        # We ignore this for now and always log at level INFO
        pass


def delete_docker_entities(entity, label):
    ls_args = [
        "docker",
        entity,
        "ls",
        "--all" if entity == "container" else None,
        "--filter",
        f"label={label}",
        "--quiet",
    ]
    ls_args = list(filter(None, ls_args))
    response = subprocess.run(ls_args, capture_output=True, encoding="ascii")
    ids = response.stdout.split()
    if ids and response.returncode == 0:
        rm_args = ["docker", entity, "rm", "--force"] + ids
        subprocess.run(rm_args, capture_output=True)
