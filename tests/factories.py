import base64
import secrets
import subprocess
import time
from collections import defaultdict
from copy import deepcopy

from jobrunner import config, record_stats, tracing
from jobrunner.controller import task_api
from jobrunner.job_executor import ExecutorState, JobResults, JobStatus
from jobrunner.lib import docker
from jobrunner.lib.database import insert
from jobrunner.models import Job, JobRequest, SavedJobRequest, State, StatusCode, Task
from jobrunner.run import job_to_job_definition
from jobrunner.schema import TaskType
from tests.conftest import test_exporter


JOB_REQUEST_DEFAULTS = {
    "repo_url": "repo",
    "commit": "commit",
    "requested_actions": ["action"],
    "cancelled_actions": [],
    "workspace": "workspace",
    "codelists_ok": True,
    "database_name": "default",
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

    if "started_at" not in kwargs:
        status_code = kwargs.get("status_code", values["status_code"])
        if status_code and status_code >= StatusCode.EXECUTING:
            values["started_at"] = int(timestamp)

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


def job_results_factory(timestamp_ns=None, **kwargs):
    if timestamp_ns is None:
        timestamp_ns = time.time_ns()
    values = deepcopy(JOB_RESULTS_DEFAULTS)
    values.update(kwargs)
    return JobResults(timestamp_ns=timestamp_ns, **values)


def metrics_factory(job=None, metrics=None):
    if job is None:
        job = job_factory()
    if metrics is None:
        metrics = {}

    record_stats.write_job_metrics(job.id, metrics)


def runjob_task_factory(*args, state=State.RUNNING, **kwargs):
    """Set up a job and corresponding task"""
    job = job_factory(*args, state=state, **kwargs)
    task = Task(
        id=job.id, type=TaskType.RUNJOB, definition=job_to_job_definition(job).to_dict()
    )
    task_api.insert_task(task)
    return task


class StubExecutorAPI:
    """Dummy implementation of the ExecutorAPI, for use in tests.

    It tracks the current state of any jobs based the calls to the various API
    methods, and get_status() will return the current JobStatus.

    You can inject new jobs to the executor with add_test_job(), for which you
    must supply a current ExecutorState and also job State.

    By default, transition methods successfully move to the next state. If you
    want to change that, call set_job_transition(job, state), and the next
    transition method call for that job will instead return that state.

    It also tracks which methods were called with which job ids to check the
    correct series of methods was invoked.

    """

    synchronous_transitions = []

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
        self.job_statuses = {}
        self.deleted = defaultdict(lambda: defaultdict(list))
        self.last_time = int(time.time())

    def add_test_job(
        self,
        executor_state,
        job_state,
        status_code=StatusCode.CREATED,
        message="message",
        timestamp=None,
        **kwargs,
    ):
        """Create and track a db job object."""

        job = job_factory(state=job_state, status_code=status_code, **kwargs)
        if executor_state != ExecutorState.UNKNOWN:
            self.set_job_status_from_executor_state(job, executor_state, message)
        return job

    def set_job_status_from_executor_state(
        self, job_definition, executor_state, message="message", timestamp_ns=None
    ):
        """Directly set a job status from an ExecutorState."""
        # handle the synchronous state meaning the state has completed
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()
        synchronous = getattr(self, "synchronous_transitions", [])
        if executor_state in synchronous:
            if executor_state == ExecutorState.PREPARING:
                executor_state = ExecutorState.PREPARED
            if executor_state == ExecutorState.FINALIZING:
                executor_state = ExecutorState.FINALIZED
        self.job_statuses[job_definition.id] = JobStatus(
            executor_state, message, timestamp_ns
        )

    def set_job_transition(
        self, job_definition, executor_state, message="executor message", hook=None
    ):
        """Set the next transition for this job when called"""
        self.transitions[job_definition.id] = (executor_state, message, hook)

    def set_job_result(self, job_definition, timestamp_ns=None, **kwargs):
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()
        defaults = {
            "outputs": {},
            "unmatched_patterns": [],
            "unmatched_outputs": [],
            "exit_code": 0,
            "image_id": "image_id",
            "message": "message",
        }
        kwargs = {**defaults, **kwargs}
        self.results[job_definition.id] = JobResults(**kwargs)

    def do_transition(
        self,
        job_definition,
        expected_executor_state,
        next_executor_state,
        transition="",
    ):
        current_job_status = self.get_status(job_definition)
        if current_job_status.state != expected_executor_state:
            executor_state = current_job_status.state
            message = f"Invalid transition {transition} to {next_executor_state}, currently state is {current_job_status.state}"
        elif job_definition.id in self.transitions:
            executor_state, message, hook = self.transitions.pop(job_definition.id)
            if hook:
                hook(job_definition)
        else:
            executor_state = next_executor_state
            message = "executor message"

        timestamp_ns = time.time_ns()
        self.set_job_status_from_executor_state(
            job_definition, executor_state, message, timestamp_ns
        )
        return JobStatus(executor_state, message, timestamp_ns)

    def prepare(self, job_definition):
        self.tracker["prepare"].add(job_definition.id)
        if ExecutorState.PREPARING in self.synchronous_transitions:
            next_executor_state = ExecutorState.PREPARED
        else:
            next_executor_state = ExecutorState.PREPARING

        return self.do_transition(
            job_definition, ExecutorState.UNKNOWN, next_executor_state, "prepare"
        )

    def execute(self, job_definition):
        self.tracker["execute"].add(job_definition.id)
        return self.do_transition(
            job_definition, ExecutorState.PREPARED, ExecutorState.EXECUTING, "execute"
        )

    def finalize(self, job_definition):
        if self.get_status(job_definition).state == ExecutorState.UNKNOWN:
            # job was cancelled before it started running
            assert job_definition.cancelled
            return self.get_status(job_definition)

        if ExecutorState.FINALIZING in self.synchronous_transitions:
            next_executor_state = ExecutorState.FINALIZED
        else:
            next_executor_state = ExecutorState.FINALIZING

        self.tracker["finalize"].add(job_definition.id)

        return self.do_transition(
            job_definition, ExecutorState.EXECUTED, next_executor_state, "finalize"
        )

    def terminate(self, job_definition):
        self.tracker["terminate"].add(job_definition.id)
        if self.get_status(job_definition).state == ExecutorState.UNKNOWN:
            # job was cancelled before it started running
            return self.do_transition(
                job_definition,
                ExecutorState.UNKNOWN,
                ExecutorState.UNKNOWN,
                "terminate",
            )
        elif self.get_status(job_definition).state == ExecutorState.PREPARED:
            # job was cancelled after it was prepared, but before it started running
            # We do not need to terminate, so proceed directly to FINALIZED
            return self.do_transition(
                job_definition,
                ExecutorState.PREPARED,
                ExecutorState.FINALIZED,
                "terminate",
            )
        else:
            # job was cancelled after it started running
            return self.do_transition(
                job_definition,
                ExecutorState.EXECUTING,
                ExecutorState.EXECUTED,
                "terminate",
            )

    def cleanup(self, job_definition):
        self.tracker["cleanup"].add(job_definition.id)
        self.job_statuses.pop(job_definition.id, None)
        # TODO: this currently does a silent error in some tests, if the initial
        # ExecutorState is not ERROR
        return self.do_transition(
            job_definition, ExecutorState.ERROR, ExecutorState.UNKNOWN, "cleanup"
        )

    def get_status(self, job_definition):
        return self.job_statuses.get(
            job_definition.id, JobStatus(ExecutorState.UNKNOWN)
        )

    def get_results(self, job_definition):
        return self.results.get(job_definition.id)

    def delete_files(self, workspace, privacy, files):
        self.deleted[workspace][privacy].extend(files)


def ensure_docker_images_present(*images):
    for image in images:
        full_image = f"{config.DOCKER_REGISTRY}/{image}"
        if not docker.image_exists_locally(full_image):
            subprocess.run(["docker", "pull", "--quiet", full_image], check=True)
