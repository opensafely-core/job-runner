import base64
import secrets
import subprocess
import time
from copy import deepcopy

from jobrunner import record_stats, tracing
from jobrunner.config import common as common_config
from jobrunner.controller import task_api
from jobrunner.controller.main import job_to_job_definition
from jobrunner.job_executor import JobResults
from jobrunner.lib import docker
from jobrunner.lib.database import insert
from jobrunner.models import Job, JobRequest, SavedJobRequest, State, StatusCode, Task
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


def runjob_db_task_factory(*args, state=State.RUNNING, **kwargs):
    """Set up a job and corresponding task"""
    job = job_factory(*args, state=state, **kwargs)
    task = Task(
        id=job.id,
        backend="test",
        type=TaskType.RUNJOB,
        definition=job_to_job_definition(job).to_dict(),
    )
    task_api.insert_task(task)
    return task


def canceljob_db_task_factory(*args, state=State.RUNNING, **kwargs):
    """Set up a job and corresponding task"""
    job = job_factory(*args, state=state, cancelled=True, **kwargs)
    task = Task(
        id=job.id,
        backend="test",
        type=TaskType.CANCELJOB,
        definition=job_to_job_definition(job).to_dict(),
    )
    task_api.insert_task(task)
    return task


def job_definition_factory(*args, **kwargs):
    job = job_factory(*args, **kwargs)
    return job_to_job_definition(job)


def ensure_docker_images_present(*images):
    for image in images:
        full_image = f"{common_config.DOCKER_REGISTRY}/{image}"
        if not docker.image_exists_locally(full_image):
            subprocess.run(["docker", "pull", "--quiet", full_image], check=True)
