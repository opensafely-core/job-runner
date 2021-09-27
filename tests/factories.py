import base64
import secrets

from jobrunner import job_executor
from jobrunner.models import Job, State
from jobrunner.lib.database import insert
from jobrunner.manage_jobs import JobError


DEFAULTS = {
    "action": "action_name",
    "repo_url": "opensafely/study",
    "workspace": "workspace",
    "requires_outputs_from": [],
    "run_command": "python myscript.py",
    "output_spec": {},
}


def job_factory(**kwargs):
    values = DEFAULTS.copy()
    values.update(kwargs)
    if "job_request_id" not in values:
        # use random job_request_id as id is derived from it
        values["job_request_id"] = (
            base64.b32encode(secrets.token_bytes(10)).decode("ascii").lower()
        )
    return Job(**values)


class TestJobAPI:
    def __init__(self):
        self.jobs_run = {}
        self.jobs_status = {}
        self.jobs_terminated = {}
        self.jobs_cleaned = {}
        self.results = {}
        self.errors = {}

    def add_test_job(self, **kwargs):
        """Create and track a db job object."""
        job = job_factory(**kwargs)
        # various code paths check the db, e.g. what's the state of my
        # dependencies?, so we inject the test job into the db also.
        insert(job)
        if job.state == State.RUNNING:
            self.jobs_run[job.id] = job
        return job

    def add_job_exception(self, job_id, exc):
        self.errors[job_id] = exc

    def add_job_result(self, job_id, state, code=None, message=None, outputs={}):
        self.results[job_id] = job_executor.JobResults(state, code, message, outputs)

    def run(self, definition):
        """Track this definition."""
        self.jobs_run[definition.id] = definition
        if definition.id in self.errors:
            raise self.errors[definition.id]

    def terminate(self, definition):
        if definition.id not in self.jobs_run:
            return
        self.jobs_terminated[definition.id] = definition

        # automatically mark this job as having failed
        self.add_job_result(definition.id, State.FAILED)

    def get_status(self, definition):
        if definition.id not in self.jobs_run:
            raise JobError(f"unknown job {definition.id}")

        if definition.id in self.errors:
            raise self.errors[definition.id]
        elif definition.id in self.results:
            result = self.results[definition.id]
            return result.state, result
        else:
            return State.RUNNING, None

    def cleanup(self, definition):
        self.jobs_cleaned[definition.id] = definition


class TestWorkspaceAPI:
    def delete_files(self, workspace, privacy, paths):
        raise NotImplemented
