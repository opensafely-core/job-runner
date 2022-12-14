import base64
import secrets
import time
from collections import defaultdict
from copy import deepcopy

from jobrunner import config, tracing
from jobrunner.job_executor import ExecutorState, JobResults, JobStatus
from jobrunner.lib import docker
from jobrunner.lib.database import insert
from jobrunner.lib.subprocess_utils import subprocess_run
from jobrunner.models import Job, JobRequest, SavedJobRequest, State, StatusCode
from tests.conftest import test_exporter


JOB_REQUEST_DEFAULTS = {
    "repo_url": "repo",
    "commit": "commit",
    "requested_actions": ["action"],
    "cancelled_actions": [],
    "workspace": "workspace",
    "database_name": "full",
    "original": {
        "created_by": "testuser",
        "project": "project",
        "orgs": ["org1", "org2"],
    },
}


JOB_DEFAULTS = {
    "state": State.PENDING,
    "action": "action_name",
    "repo_url": "opensafely/study",
    "workspace": "workspace",
    "requires_outputs_from": ["some-earlier-action"],
    "run_command": "python myscript.py",
    "output_spec": {},
    "created_at": 0,
    "status_code": StatusCode.CREATED,
}


JOB_RESULTS_DEFAULTS = {
    "outputs": ["output1", "output2"],
    "unmatched_patterns": [],
    "unmatched_outputs": [],
    "exit_code": 0,
    "image_id": "image_id",
    "message": "message",
}


def job_request_factory_raw(**kwargs):
    if "id" not in kwargs:
        kwargs["id"] = base64.b32encode(secrets.token_bytes(10)).decode("ascii").lower()

    values = deepcopy(JOB_REQUEST_DEFAULTS)
    values.update(kwargs)
    return JobRequest(**values)


def job_request_factory(**kwargs):
    job_request = job_request_factory_raw(**kwargs)
    insert(SavedJobRequest(id=job_request.id, original=job_request.original))
    return job_request


def job_factory(job_request=None, **kwargs):
    if job_request is None:
        job_request = job_request_factory()

    values = deepcopy(JOB_DEFAULTS)
    # default times
    timestamp = time.time()
    if "created_at" not in kwargs:
        values["created_at"] = int(timestamp)
    if "updated_at" not in kwargs:
        values["updated_at"] = int(timestamp)
    if "status_code_updated_at" not in kwargs:
        values["status_code_updated_at"] = int(timestamp * 1e9)
    values.update(kwargs)

    values["job_request_id"] = job_request.id
    job = Job(**values)

    # initialise tracing
    tracing.initialise_trace(job)

    insert(job)

    # ensure tests just have the span they generate
    test_exporter.clear()
    return job


def job_results_factory(**kwargs):
    values = deepcopy(JOB_RESULTS_DEFAULTS)
    values.update(kwargs)
    return JobResults(**values)


class StubExecutorAPI:
    """Dummy implementation of the ExecutorAPI, for use in tests.

    It tracks the current state of any jobs based the calls to the various API
    methods, and get_status() will return the current state.

    You can inject new jobs to the executor with add_test_job(), for which you
    must supply a current ExecutorState and also job State.

    By default, transition methods successfully move to the next state. If you
    want to change that, call set_job_transition(job, state), and the next
    transition method call for that job will instead return that state.

    It also tracks which methods were called with which job ids to check the
    correct series of methods was invoked.

    """

    def __init__(self):

        self.tracker = {
            "prepare": set(),
            "execute": set(),
            "finalize": set(),
            "terminate": set(),
            "cleanup": set(),
        }
        self.transitions = {}
        self.results = {}
        self.state = {}
        self.deleted = defaultdict(lambda: defaultdict(list))

    def add_test_job(
        self,
        exec_state,
        job_state,
        status_code=StatusCode.CREATED,
        message="message",
        timestamp=None,
        **kwargs,
    ):
        """Create and track a db job object."""

        job = job_factory(state=job_state, status_code=status_code, **kwargs)
        if exec_state != ExecutorState.UNKNOWN:
            self.state[job.id] = JobStatus(
                exec_state, message, timestamp or int(time.time())
            )
        return job

    def set_job_state(self, definition, state, message="message", timestamp=None):
        """Directly set a job state."""
        # handle the synchronous state meaning the state has completed
        synchronous = getattr(self, "synchronous_transitions", [])
        if state in synchronous:
            if state == ExecutorState.PREPARING:
                state = ExecutorState.PREPARED
            if state == ExecutorState.FINALIZING:
                state = ExecutorState.FINALIZED
        if timestamp is None:
            timestamp = time.time()
        self.state[definition.id] = JobStatus(state, message, timestamp)

    def set_job_transition(
        self, definition, state, message="executor message", timestamp=None
    ):
        """Set the next transition for this job when called"""
        self.transitions[definition.id] = (state, message, timestamp)

    def set_job_result(self, definition, **kwargs):
        defaults = {
            "outputs": {},
            "unmatched_patterns": [],
            "unmatched_outputs": [],
            "exit_code": 0,
            "image_id": "image_id",
            "message": "message",
        }
        kwargs = {**defaults, **kwargs}
        self.results[definition.id] = JobResults(**kwargs)

    def do_transition(self, definition, expected, next_state):
        current = self.get_status(definition)
        timestamp = int(time.time())
        if current.state != expected:
            state = current.state
            message = f"Invalid transition to {next_state}, currently state is {current.state}"
        elif definition.id in self.transitions:
            state, message, timestamp = self.transitions[definition.id]
        else:
            state = next_state
            message = "executor message"

        self.set_job_state(definition, state, message, timestamp)
        return JobStatus(state, message, timestamp)

    def prepare(self, definition):
        self.tracker["prepare"].add(definition.id)
        return self.do_transition(
            definition, ExecutorState.UNKNOWN, ExecutorState.PREPARING
        )

    def execute(self, definition):
        self.tracker["execute"].add(definition.id)
        return self.do_transition(
            definition, ExecutorState.PREPARED, ExecutorState.EXECUTING
        )

    def finalize(self, definition):
        self.tracker["finalize"].add(definition.id)
        return self.do_transition(
            definition, ExecutorState.EXECUTED, ExecutorState.FINALIZING
        )

    def terminate(self, definition):
        self.tracker["terminate"].add(definition.id)
        return JobStatus(ExecutorState.ERROR)

    def cleanup(self, definition):
        self.tracker["cleanup"].add(definition.id)
        self.state.pop(definition.id, None)
        return JobStatus(ExecutorState.UNKNOWN)

    def get_status(self, definition):
        return self.state.get(definition.id, JobStatus(ExecutorState.UNKNOWN))

    def get_results(self, definition):
        return self.results.get(definition.id)

    def delete_files(self, workspace, privacy, files):
        self.deleted[workspace][privacy].extend(files)


def ensure_docker_images_present(*images):
    for image in images:
        full_image = f"{config.DOCKER_REGISTRY}/{image}"
        if not docker.image_exists_locally(full_image):
            subprocess_run(["docker", "pull", "--quiet", full_image], check=True)
