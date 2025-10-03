"""
Big integration tests that create a basic project in a git repo, mocks out a
JobRequest from the job-server to run it, and then exercises the sync and run
loops to run entire pipeline
"""

import json
import logging

import pytest

import agent.main
import controller.main
import controller.sync
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
    # RAP API stage 2: Use `emis` rather than `test` so that we can disable the
    # job-runner sync loop for `test`
    monkeypatch.setattr("agent.config.BACKEND", "emis")
    monkeypatch.setattr("agent.config.TASK_API_TOKEN", "test_token")
    # set all the tmp workdir config as we remove it for the controller phase
    set_tmp_workdir_config(monkeypatch, tmp_work_dir)

    # disable controller config
    # (note some of these will be set in prod because they are based on shared config
    # e.g. MAX_WORKERS is based on BACKENDS so will always be populated for each
    # backend, with the default value. We set it explicitly here to confirm that it
    # doesn't trigger any errors if it is invalid for the agent i.e. it's not used)
    monkeypatch.setattr("controller.config.JOB_SERVER_ENDPOINT", None)
    monkeypatch.setattr("controller.config.MAX_WORKERS", None)

    # This is controller config, but we need it to be set during the agent part of the
    # test, as the agent will call the controller app
    monkeypatch.setattr("controller.config.JOB_SERVER_TOKENS", {"emis": "test_token"})


def set_controller_config(monkeypatch):
    # set controller config
    monkeypatch.setattr(
        "controller.config.JOB_SERVER_ENDPOINT", "http://testserver/api/v2/"
    )
    monkeypatch.setattr("controller.config.JOB_SERVER_TOKENS", {"emis": "token"})
    # Ensure that we have enough workers to start the jobs we expect in the test
    # (CI may have fewer actual available workers than this)
    monkeypatch.setattr("controller.config.MAX_WORKERS", {"emis": 4})
    monkeypatch.setattr("controller.config.MAX_DB_WORKERS", {"emis": 2})
    monkeypatch.setattr("controller.config.DEFAULT_JOB_CPU_COUNT", {"emis": 2.0})
    monkeypatch.setattr("controller.config.DEFAULT_JOB_MEMORY_LIMIT", {"emis": "4G"})

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

    # Special case for the RAP API v2 initiative.
    monkeypatch.setattr("controller.create_or_update_jobs.SKIP_CANCEL_FOR_BACKEND", "")


@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_integration(
    live_server, tmp_work_dir, docker_cleanup, monkeypatch, test_repo, responses
):
    api = get_executor_api()
    monkeypatch.setattr("common.config.BACKENDS", ["emis"])
    monkeypatch.setattr("common.config.JOB_LOOP_INTERVAL", 0)

    # Use the live_server url for our task api endpoint, so we can test the
    # agent calls to the django app endpoints
    monkeypatch.setattr("agent.config.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    ensure_docker_images_present("ehrql:v1", "python")

    # Set up a mock job-server with a single job request
    job_request_1 = {
        "identifier": 1,
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
        "backend": "emis",
    }

    responses.add(
        method="GET",
        url="http://testserver/api/v2/job-requests/",
        status=200,
        json={"results": [job_request_1]},
    )

    responses.add(
        method="POST", url="http://testserver/api/v2/jobs/", status=200, json={}
    )

    # START ON CONTROLLER; set up the expected controller config (and remove agent config)
    set_controller_config(monkeypatch)
    # Run sync to grab the JobRequest from the mocked job-server
    controller.sync.sync()
    # Check that expected number of pending jobs are created
    jobs = get_posted_jobs(responses)
    for job in jobs.values():
        assert job["status"] == "pending"
        assert job["status_code"] == "created"
        assert job["started_at"] is None

    # no active tasks yet
    assert not get_active_db_tasks()

    # Execute one tick of the controller run loop and then sync
    # The controller creates one runjob task, for the one action that has no
    # dependencies, and marks that job as running
    controller.main.handle_jobs()
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 1
    assert active_tasks[0].type == TaskType.RUNJOB
    assert active_tasks[0].id.startswith(jobs["generate_dataset"]["identifier"])
    # stage is None before the task has been picked up by the agent
    assert active_tasks[0].agent_stage is None

    controller.sync.sync()

    # We should now have one running (initiated, i.e. task created) job and all others waiting on dependencies
    jobs = get_posted_jobs(responses)
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
    # sync again
    controller.sync.sync()
    # still one running job (now prepared) and all others waiting on dependencies
    jobs = get_posted_jobs(responses)
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
    # sync again
    controller.sync.sync()
    # still one running job (now executing) and all others waiting on dependencies
    jobs = get_posted_jobs(responses)
    assert_generate_dataset_dependency_running(jobs, "executing")

    # Update the existing job request to mark a (not-started) job as cancelled, add a new job
    # request to be run and then sync
    job_request_1["cancelled_actions"] = ["test_cancellation_ehrql"]
    job_request_2 = {
        "identifier": 2,
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
        "backend": "emis",
    }
    responses.add(
        method="GET",
        url="http://testserver/api/v2/job-requests/",
        status=200,
        json={"results": [job_request_1, job_request_2]},
    )

    controller.sync.sync()

    # Execute one tick of the controller run loop again to pick up the
    # cancelled job and the second job request and then sync
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

    # sync to confirm updated jobs have been posted back to job-server
    controller.sync.sync()
    jobs = get_posted_jobs(responses)
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

    controller.sync.sync()
    jobs = get_posted_jobs(responses)
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

    controller.sync.sync()
    jobs = get_posted_jobs(responses)
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

    controller.sync.sync()
    jobs = get_posted_jobs(responses)
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


def get_posted_jobs(responses):
    data = json.loads(responses.calls[-1].request.body)
    return {job["action"]: job for job in data}


def get_active_db_tasks():
    return find_where(Task, active=True)
