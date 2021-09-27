import base64
import secrets

from jobrunner import job_executor
from jobrunner.models import Job, JobRequest, SavedJobRequest, State
from jobrunner.lib.database import insert
from jobrunner.manage_jobs import JobError


JOB_REQUEST_DEFAULTS = {
    "repo_url": "repo",
    "commit": "commit",
    "requested_actions": [],
    "cancelled_actions": [],
    "workspace": "workspace",
    "database_name": "full",
    "original": {
        "created_by": "testuser",
    },
}


JOB_DEFAULTS = {
    "action": "action_name",
    "repo_url": "opensafely/study",
    "workspace": "workspace",
    "requires_outputs_from": [],
    "run_command": "python myscript.py",
    "output_spec": {},
}


def job_request_factory(**kwargs):
    if "id" not in kwargs:
        kwargs["id"] = base64.b32encode(secrets.token_bytes(10)).decode("ascii").lower()

    values = JOB_REQUEST_DEFAULTS.copy()
    values.update(kwargs)
    job_request = JobRequest(**values)
    insert(SavedJobRequest(id=job_request.id, original=job_request.original))
    return job_request


def job_factory(job_request=None, **kwargs):
    if job_request is None:
        job_request = job_request_factory()

    values = JOB_DEFAULTS.copy()
    values.update(kwargs)
    values["job_request_id"] = job_request.id
    job = Job(**values)
    insert(job)
    return job


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
        if job.state == State.RUNNING:
            self.jobs_run[job.id] = job
        return job

    def add_job_exception(self, job_id, exc):
        self.errors[job_id] = exc

    def add_job_result(
        self,
        job_id,
        state,
        code=None,
        message=None,
        outputs={},
        exit_code=0,
        image_id="image_id",
    ):
        self.results[job_id] = job_executor.JobResults(
            state, code, message, outputs, exit_code, image_id
        )

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
