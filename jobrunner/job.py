"""
NOTE: This module exists purely as a temporary shim to fake enough of the old
job-runner API to keep the cohortextractor integration working unchanged.
"""
import sys

from . import local_run


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
        self.action_id = job_spec["action_id"]
        self.force_run_dependencies = job_spec["force_run_dependencies"]
        self.workdir = workdir

    def main(self):
        success = local_run.main(
            project_dir=self.workdir,
            actions=[self.action_id],
            force_run_dependencies=self.force_run_dependencies,
        )
        sys.exit(0 if success else 1)

    # We need to support `job.logger.setLevel()` and this is the easiest way to
    # do this
    @property
    def logger(self):
        return self

    def setLevel(self, log_level):
        # We ignore this for now and always log at level INFO
        pass
