import base64
import secrets
import subprocess
import time
from copy import deepcopy

from agent import metrics
from agent.lib import docker
from common import config as common_config
from common.schema import JobTaskResults, TaskType
from controller import task_api, tracing
from controller.lib.database import count_where, insert, update
from controller.main import create_task_for_job, job_to_job_definition
from controller.models import Job, State, StatusCode, Task
from controller.webapp.views.validators.dataclasses import CreateRequest
from tests.conftest import test_exporter


DEFAULT_COMMIT = "commit"
DEFAULT_REPO = "repo"

CREATE_REQUEST_DEFAULTS = {
    "repo_url": DEFAULT_REPO,
    "commit": DEFAULT_COMMIT,
    "branch": "main",
    "requested_actions": ["action"],
    "workspace": "workspace",
    "codelists_ok": True,
    "database_name": "default",
    "backend": "test",
    "force_run_dependencies": True,
    "created_by": "testuser",
    "project": "project",
    "orgs": ["org1", "org2"],
    "analysis_scope": {},
}

# Represents a RAP request body received at /controller/v1/rap/create
RAP_API_V1_DEFAULTS = {
    "repo_url": DEFAULT_REPO,
    "commit": "aaaaaaaaaabbbbbbbbbb11111111112222222222",  # needs to be [0-9a-f]{40} for validation
    "requested_actions": ["action"],
    "workspace": "workspace",
    "branch": "main",
    "codelists_ok": True,
    "force_run_dependencies": False,
    "database_name": "default",
    "backend": "test",
    "created_by": "test_user",
    "project": "test_project",
    "orgs": ["test_org"],
    "analysis_scope": {},
}


JOB_DEFAULTS = {
    "state": State.PENDING,
    "action": "action_name",
    "requires_outputs_from": ["some-earlier-action"],
    "run_command": "python:v2 myscript.py",
    "output_spec": {},
    "created_at": 0,
    "status_code": StatusCode.CREATED,
    "backend": "test",
    "branch": "main",
    "user": "testuser",
    "project": "project",
    "orgs": ["org1", "org2"],
}


JOB_TASK_RESULTS_DEFAULTS = {
    "has_unmatched_patterns": False,
    "has_level4_excluded_files": False,
    "exit_code": 0,
    "image_id": "image_id",
    "message": "message",
}


def rap_create_request_factory(**kwargs):
    if "id" not in kwargs:
        kwargs["id"] = base64.b32encode(secrets.token_bytes(10)).decode("ascii").lower()

    values = deepcopy(CREATE_REQUEST_DEFAULTS)
    values.update(kwargs)
    return CreateRequest(**values)


def rap_api_v1_factory_raw(**kwargs):
    if "rap_id" not in kwargs:
        kwargs["rap_id"] = (
            base64.b32encode(secrets.token_bytes(10)).decode("ascii").lower()
        )

    values = deepcopy(RAP_API_V1_DEFAULTS)
    values.update(kwargs)
    return values


def job_factory(rap_create_request=None, **kwargs):
    if rap_create_request is None:
        # if there's a job backend, make sure the job request is consistent
        rap_create_request_kwargs = {}
        if job_backend := kwargs.get("backend"):
            rap_create_request_kwargs = {"backend": job_backend}
        rap_create_request = rap_create_request_factory(**rap_create_request_kwargs)

    values = deepcopy(JOB_DEFAULTS)
    values["workspace"] = rap_create_request.workspace
    values["repo_url"] = rap_create_request.repo_url
    values["commit"] = rap_create_request.commit

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

    values["rap_id"] = rap_create_request.id
    job = Job(**values)

    # initialise tracing
    tracing.initialise_job_trace(job)

    insert(job)

    # ensure tests just have the span they generate
    test_exporter.clear()
    return job


def job_task_results_factory(timestamp_ns=None, **kwargs):
    if timestamp_ns is None:
        timestamp_ns = time.time_ns()
    values = deepcopy(JOB_TASK_RESULTS_DEFAULTS)
    values.update(kwargs)
    return JobTaskResults(timestamp_ns=timestamp_ns, **values)


def metrics_factory(job_id, m=None):
    if job_id is None:
        job = job_factory()
        job_id = job.id
    if m is None:
        m = {}

    metrics.write_job_metrics(job_id, m)


def runjob_db_task_factory(job=None, *, backend="test", **kwargs):
    """Set up a job and corresponding task"""
    if job is None:
        # default to RUNNING, as no task is created for PENDING by default
        job = job_factory(state=State.RUNNING, backend=backend)
    task = create_task_for_job(job)
    for k, v in kwargs.items():
        setattr(task, k, v)

    task_api.insert_task(task)

    # insert_task always sets active=true. If we want to create an inactive
    # task, we need to modify it post insertion.
    if kwargs.get("active") is False:
        task.active = False
        update(task)

    return task


def canceljob_db_task_factory(job=None, *, backend="test", **kwargs):
    """Set up a job and corresponding task"""
    if job is None:
        job = job_factory(state=State.RUNNING, cancelled=True, backend=backend)
    previous_task_count = count_where(Task, id__glob=f"{job.id}-*", backend=job.backend)
    task_id = f"{job.id}-00{previous_task_count + 1}"
    task = Task(
        id=task_id,
        backend=backend,
        type=TaskType.CANCELJOB,
        definition=job_to_job_definition(job, task_id).to_dict(),
        **kwargs,
    )
    task_api.insert_task(task)

    # insert_task always sets active=true. If we want to create an inactive
    # task, we need to modify it post insertion.
    if kwargs.get("active") is False:
        task.active = False
        update(task)

    return task


def job_definition_factory(*args, **kwargs):
    task_id = kwargs.pop("task_id", "")
    job = job_factory(*args, **kwargs)
    return job_to_job_definition(job, task_id)


def ensure_docker_images_present(*images):
    for image in images:
        full_image = f"{common_config.DOCKER_REGISTRY}/{image}"
        if not docker.image_exists_locally(full_image):
            subprocess.run(["docker", "pull", "--quiet", full_image], check=True)
