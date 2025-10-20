"""
Big integration tests that create a basic project in a git repo, mocks out a
JobRequest from the job-server to run it, and then exercises the sync and run
loops to run entire pipeline
"""

import json
import logging

import pytest
import requests

import agent.main
import controller.main
from agent.executors import get_executor_api
from common.schema import TaskType
from controller.lib.database import find_where
from controller.models import Task
from tests.conftest import get_trace, set_tmp_workdir_config
from tests.factories import ensure_docker_images_present


log = logging.getLogger(__name__)


def set_agent_config(monkeypatch, tmp_work_dir):
    # set agent config
    monkeypatch.setattr("agent.config.USING_DUMMY_DATA_BACKEND", True)
    # Note that as we are running ehrql actions in this test, we need to set
    # the backend to a value that ehrql will accept
    monkeypatch.setattr("agent.config.BACKEND", "test")
    monkeypatch.setattr("agent.config.TASK_API_TOKEN", "test_token")
    # set all the tmp workdir config as we remove it for the controller phase
    set_tmp_workdir_config(monkeypatch, tmp_work_dir)

    # disable controller config
    # (note some of these will be set in prod because they are based on shared config
    # e.g. MAX_WORKERS is based on BACKENDS so will always be populated for each
    # backend, with the default value. We set it explicitly here to confirm that it
    # doesn't trigger any errors if it is invalid for the agent i.e. it's not used)
    monkeypatch.setattr("controller.config.MAX_WORKERS", None)

    # This is controller config, but we need it to be set during the agent part of the
    # test, as the agent will call the controller app
    monkeypatch.setattr("controller.config.JOB_SERVER_TOKENS", {"test": "test_token"})


def set_controller_config(monkeypatch):
    # set controller config
    monkeypatch.setattr("controller.config.JOB_SERVER_TOKENS", {"test": "token"})
    # Ensure that we have enough workers to start the jobs we expect in the test
    # (CI may have fewer actual available workers than this)
    monkeypatch.setattr("controller.config.MAX_WORKERS", {"test": 4})

    # disable agent config
    # (note some of these will be set in prod because they are based on shared config
    # e.g. HIGH_PRIVACY_STORAGE_BASE is based on WORKDIR which is a common config. We
    # set it explicitly here to confirm that it doesn't trigger any errors if it is
    # invalid for the controller i.e. it's not used.)
    monkeypatch.setattr("agent.config.BACKEND", None)
    monkeypatch.setattr("agent.config.USING_DUMMY_DATA_BACKEND", False)

    config_vars = [
        "TMP_DIR",
        "HIGH_PRIVACY_STORAGE_BASE",
        "MEDIUM_PRIVACY_STORAGE_BASE",
        "HIGH_PRIVACY_WORKSPACES_DIR",
        "MEDIUM_PRIVACY_WORKSPACES_DIR",
        "HIGH_PRIVACY_ARCHIVE_DIR",
        "HIGH_PRIVACY_VOLUME_DIR",
        "JOB_LOG_DIR",
        "TASK_API_TOKEN",
    ]

    for config_var in config_vars:
        monkeypatch.setattr(f"agent.config.{config_var}", None)

    # Client tokens for calls to the RAP API (controller webapp)
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})


@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_integration(
    live_server, tmp_work_dir, docker_cleanup, monkeypatch, test_repo, responses
):
    api = get_executor_api()
    monkeypatch.setattr("common.config.BACKENDS", ["test"])
    monkeypatch.setattr("common.config.JOB_LOOP_INTERVAL", 0)

    # Use the live_server url for our task api endpoint, so we can test the
    # agent calls to the django app endpoints
    monkeypatch.setattr("agent.config.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    ensure_docker_images_present("ehrql:v1", "python:v2")

    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    # Set up a mock job-server with a single job request
    job_request_1 = {
        "identifier": "12345678abcdefgh",
        "requested_actions": [
            "analyse_data_ehrql",
            "test_reusable_action_ehrql",
            "test_cancellation_ehrql",
        ],
        "cancelled_actions": [],
        "force_run_dependencies": False,
        "workspace": {
            "name": "testing",
            "repo": str(test_repo.path),
            "branch": "main",
        },
        "codelists_ok": True,
        "database_name": "default",
        "sha": test_repo.commit,
        "created_by": "user",
        "project": "project",
        "orgs": ["org"],
        "backend": "test",
    }

    # START ON CONTROLLER; set up the expected controller config (and remove agent config)
    set_controller_config(monkeypatch)

    # Mock job-server post to the RAP API create endpoint to create jobs
    headers = {"Authorization": "test_token"}
    create_response = create_jobs_via_api(live_server, headers, job_request_1)
    # See test/fixtures/full_project/project.yaml - the 3 requests actions require 7 jobs
    # to be created
    assert create_response["count"] == 7

    # Check that expected number of pending jobs are created via status API
    jobs = check_status_via_api(live_server, headers, [job_request_1["identifier"]])
    for job in jobs.values():
        assert job["status"] == "pending"
        assert job["status_code"] == "created"
        assert job["started_at"] is None

    # no active tasks yet
    assert not get_active_db_tasks()

    # Execute one tick of the controller run loop and then check via status API
    # The controller creates one runjob task, for the one action that has no
    # dependencies, and marks that job as running
    controller.main.handle_jobs()
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 1
    assert active_tasks[0].type == TaskType.RUNJOB
    assert active_tasks[0].id.startswith(jobs["generate_dataset"]["identifier"])
    # stage is None before the task has been picked up by the agent
    assert active_tasks[0].agent_stage is None

    # We should now have one running (initiated, i.e. task created) job and all others waiting on dependencies
    jobs = check_status_via_api(live_server, headers, [job_request_1["identifier"]])
    # started_at should not change after job is first initiated
    started_at = jobs["generate_dataset"]["started_at"]

    def assert_generate_dataset_dependency_running(jobs, running_status_code):
        assert jobs["generate_dataset"]["status"] == "running"
        assert jobs["generate_dataset"]["status_code"] == running_status_code
        assert jobs["generate_dataset"]["started_at"] == started_at
        for action in [
            "prepare_data_m_ehrql",
            "prepare_data_f_ehrql",
            "prepare_data_with_quote_in_filename_ehrql",
            "analyse_data_ehrql",
            "test_reusable_action_ehrql",
            "test_cancellation_ehrql",
        ]:
            assert jobs[action]["status_code"] == "waiting_on_dependencies"
            assert jobs[action]["status_message"].startswith("Waiting on dependencies")

    assert_generate_dataset_dependency_running(jobs, "initiated")

    # MOVE TO AGENT; set up the expected agent config (and remove controller config)
    set_agent_config(monkeypatch, tmp_work_dir)
    # Execute one tick of the agent run loop to pick up the runjob task
    # After one tick, the task should have moved to the PREPARED stage
    agent.main.handle_tasks(api)
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 1
    assert active_tasks[0].agent_stage == "prepared"

    # CONTROLLER
    set_controller_config(monkeypatch)
    # Run the controller loop again to update the job status code
    controller.main.handle_jobs()
    # still one running job (now prepared) and all others waiting on dependencies
    jobs = check_status_via_api(live_server, headers, [job_request_1["identifier"]])
    assert_generate_dataset_dependency_running(jobs, "prepared")

    # AGENT
    set_agent_config(monkeypatch, tmp_work_dir)
    # After one tick of the agent loop, the task should have moved to EXECUTING status
    agent.main.handle_tasks(api)
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 1
    assert active_tasks[0].agent_stage == "executing"

    # CONTROLLER
    set_controller_config(monkeypatch)
    # Run the controller loop again to update the job status code
    controller.main.handle_jobs()
    # still one running job (now executing) and all others waiting on dependencies
    jobs = check_status_via_api(live_server, headers, [job_request_1["identifier"]])
    assert_generate_dataset_dependency_running(jobs, "executing")

    # Update the existing job request to mark a (not-started) job as cancelled, add a new job
    # request to be run and then check status via API
    job_request_1["cancelled_actions"] = ["test_cancellation_ehrql"]
    job_request_2 = {
        "identifier": "87654321hgfedcba",
        "requested_actions": [
            "generate_dataset_with_dummy_data",
        ],
        "cancelled_actions": [],
        "force_run_dependencies": False,
        "workspace": {
            "name": "testing",
            "repo": str(test_repo.path),
            "branch": "HEAD",
        },
        "codelists_ok": True,
        "database_name": "default",
        "sha": test_repo.commit,
        "created_by": "user",
        "project": "project",
        "orgs": ["org"],
        "backend": "test",
    }

    # Call RAP API to cancel the job from the first job request
    cancel_response = cancel_job_via_api(
        live_server, headers, job_request_1["identifier"], "test_cancellation_ehrql"
    )
    assert cancel_response["count"] == 1

    # Call RAP API to create the jobs for this new job request
    create_response = create_jobs_via_api(live_server, headers, job_request_2)
    assert create_response["count"] == 1

    # Execute one tick of the controller run loop again to pick up the
    # cancelled job and the second job request and then check status via API
    # We now have 2 RUNJOB tasks (for generate_dataset, which is still executing,
    # and the new generate_dataset_with_dummy_data, which has no dependencies)
    # The cancelled job is now marked as cancelled, but no CANCELJOB task is created,
    # because no RUNJOB task had been created for it
    # The others are waiting on dependencies, so no tasks have been created for them yet
    controller.main.handle_jobs()
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 2
    assert active_tasks[0].agent_stage == "executing"
    # new task has no stage as it hasn't been picked up by agent yet
    assert active_tasks[1].agent_stage is None

    # check status
    jobs = check_status_via_api(
        live_server, headers, [job_request_1["identifier"], job_request_2["identifier"]]
    )
    assert jobs["generate_dataset"]["status"] == "running"
    assert jobs["generate_dataset"]["status_code"] == "executing"
    # The new action does not depend on generate_dataset
    assert jobs["generate_dataset_with_dummy_data"]["status"] == "running"
    assert jobs["generate_dataset_with_dummy_data"]["status_code"] == "initiated"
    cancellation_job = jobs.pop("test_cancellation_ehrql")
    assert cancellation_job["status"] == "failed"
    assert cancellation_job["status_message"] == "Cancelled by user"

    for action in [
        "prepare_data_m_ehrql",
        "prepare_data_f_ehrql",
        "prepare_data_with_quote_in_filename_ehrql",
        "analyse_data_ehrql",
        "test_reusable_action_ehrql",
    ]:
        assert jobs[action]["status_message"].startswith("Waiting on dependencies")

    # AGENT
    set_agent_config(monkeypatch, tmp_work_dir)
    # Run the agent loop until there are no active tasks left; the generate_dataset jobs should be done
    agent.main.main(exit_callback=lambda active_tasks: len(active_tasks) == 0)

    # CONTROLLER
    set_controller_config(monkeypatch)
    # Run the controller again, this should:
    # - pick up the completed task and mark generate_dataset as succeeded
    # - add RUNJOB tasks for the 4 jobs that depend on generate_dataset and set the Job state to running
    # - the analyse_data job is still pending
    controller.main.handle_jobs()
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 4
    task_ids = sorted(task.id for task in active_tasks)
    expected_job_ids = sorted(
        jobs[action]["identifier"]
        for action in [
            "test_reusable_action_ehrql",
            "prepare_data_m_ehrql",
            "prepare_data_f_ehrql",
            "prepare_data_with_quote_in_filename_ehrql",
        ]
    )
    for task_id, job_id in zip(task_ids, expected_job_ids):
        assert task_id.startswith(job_id)

    jobs = check_status_via_api(
        live_server, headers, [job_request_1["identifier"], job_request_2["identifier"]]
    )
    for action in ["generate_dataset", "generate_dataset_with_dummy_data"]:
        assert jobs[action]["status"] == "succeeded"
    for action in [
        "prepare_data_m_ehrql",
        "prepare_data_f_ehrql",
        "prepare_data_with_quote_in_filename_ehrql",
        "test_reusable_action_ehrql",
    ]:
        assert jobs[action]["status"] == "running"

    assert jobs["test_cancellation_ehrql"]["status"] == "failed"
    assert jobs["analyse_data_ehrql"]["status"] == "pending"

    # AGENT
    set_agent_config(monkeypatch, tmp_work_dir)
    # Run the agent loop until there are no active tasks left; the 4 running jobs
    # are now done
    agent.main.main(exit_callback=lambda active_tasks: len(active_tasks) == 0)

    # CONTROLLER
    set_controller_config(monkeypatch)
    # Run the controller again, this should find the 4 jobs it thinks are still running,
    # identify that their tasks are completed, and mark them as succeeded
    # And it will start a new task for the analyse_data action now that its
    # dependencies have succeeded
    controller.main.handle_jobs()

    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 1

    jobs = check_status_via_api(
        live_server, headers, [job_request_1["identifier"], job_request_2["identifier"]]
    )
    assert jobs["analyse_data_ehrql"]["status"] == "running"
    for action in [
        "prepare_data_m_ehrql",
        "prepare_data_f_ehrql",
        "prepare_data_with_quote_in_filename_ehrql",
        "test_reusable_action_ehrql",
    ]:
        assert jobs[action]["status"] == "succeeded"

    # Run the agent and controller again to complete the last job and mark it as
    # succeeded
    # AGENT
    set_agent_config(monkeypatch, tmp_work_dir)
    agent.main.main(exit_callback=lambda active_tasks: len(active_tasks) == 0)
    # CONTROLLER
    set_controller_config(monkeypatch)
    controller.main.handle_jobs()

    # no tasks left to do
    active_tasks = get_active_db_tasks()
    assert not len(active_tasks)

    jobs = check_status_via_api(
        live_server, headers, [job_request_1["identifier"], job_request_2["identifier"]]
    )
    cancellation_job = jobs.pop("test_cancellation_ehrql")
    for job in jobs.values():
        assert job["status"] == "succeeded", job

    high_privacy_workspace = tmp_work_dir / "high_privacy_workspaces_dir" / "testing"
    medium_privacy_workspace = (
        tmp_work_dir / "medium_privacy_workspaces_dir" / "testing"
    )

    # Check that the manifest contains what we expect.
    manifest_file = medium_privacy_workspace / "metadata" / "manifest.json"
    manifest = json.loads(manifest_file.read_text())
    assert manifest["workspace"] == "testing"
    assert manifest["repo"] is None

    # Check that all the outputs have been produced
    for highly_sensitive_output in [
        "output/dataset.csv",  # the cohort/dataset
        "output/extra/dataset.csv",  # extracted from dummy data
        "ehrql-male.csv",  # intermediate analysis
        "ehrql-female.csv",  # intermediate analysis
        "ehrql-qu'ote.csv",  # checking handling of problematic characters in filenames
        "output/dataset.backup.csv",  # from the reusable action
    ]:
        path = high_privacy_workspace / highly_sensitive_output
        assert path.exists(), highly_sensitive_output

    for moderately_sensitive_output in [
        "ehrql-counts.txt",  # the study's actual output
    ]:
        assert (medium_privacy_workspace / moderately_sensitive_output).exists()

    # Check that we don't produce outputs for cancelled jobs
    assert not (high_privacy_workspace / "ehrql-somefile.csv").exists()

    # Check that spans were emitted and capture details
    job_spans = [s for s in get_trace("jobs") if s.name == "JOB"]
    assert len(job_spans) == 8
    # one job is cancelled
    executed_jobs = [s for s in job_spans if "job.exit_code" in s.attributes]
    assert len(executed_jobs) == 7
    assert sum(s.attributes["job.exit_code"] for s in executed_jobs) == 0

    # If this fails, it might be that your docker images have missing labels,
    # try pulling.  If that fails, it maybe the latest images are missing
    # labels.
    assert not any(
        s.attributes["job.action_created"] == "unknown" for s in executed_jobs
    )

    loop_spans = [s for s in get_trace("agent_loop") if s.name == "AGENT_LOOP"]
    assert len(loop_spans) > 1


def check_status_via_api(live_server, headers, rap_ids):
    # Do a request to the RAP API & get the job info from there for us to verify
    # Nb. responses.add_passthrough has already been enabled for this url
    post_data = {"rap_ids": rap_ids}
    response = requests.post(
        live_server + "/controller/v1/rap/status/",
        json.dumps(post_data),
        headers=headers,
    )
    data = response.json()
    jobs = {job["action"]: job for job in data["jobs"]}
    return jobs


def create_jobs_via_api(live_server, headers, job_request_dict):
    """Do a request to the RAP API to create new jobs"""

    # Convert the job request dict (expected response from job-server job requests endpoint)
    # to the data we expect to be posted to the RAP API create endoint
    post_data = {
        "backend": job_request_dict["backend"],
        "rap_id": job_request_dict["identifier"],
        "workspace": job_request_dict["workspace"]["name"],
        "repo_url": job_request_dict["workspace"]["repo"],
        "branch": job_request_dict["workspace"]["branch"],
        "commit": job_request_dict["sha"],
        "database_name": job_request_dict["database_name"],
        "requested_actions": job_request_dict["requested_actions"],
        "codelists_ok": job_request_dict["codelists_ok"],
        "force_run_dependencies": job_request_dict["force_run_dependencies"],
        "created_by": job_request_dict["created_by"],
        "project": job_request_dict["project"],
        "orgs": job_request_dict["orgs"],
    }

    response = requests.post(
        live_server + "/controller/v1/rap/create/",
        json.dumps(post_data),
        headers=headers,
    )
    return response.json()


def cancel_job_via_api(live_server, headers, rap_id, action):
    """Do a request to the RAP API to cancel an action in a RAP."""

    post_data = {
        "rap_id": rap_id,
        "actions": [action],
    }

    response = requests.post(
        live_server + "/controller/v1/rap/cancel/",
        json.dumps(post_data),
        headers=headers,
    )
    return response.json()


def get_active_db_tasks():
    return find_where(Task, active=True)
