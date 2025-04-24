"""
Big integration tests that create a basic project in a git repo, mocks out a
JobRequest from the job-server to run it, and then exercises the sync and run
loops to run entire pipeline
"""

import json
import logging

import pytest

import jobrunner.agent.main
import jobrunner.controller.main
import jobrunner.sync
from jobrunner.executors import get_executor_api
from jobrunner.lib.database import find_where
from jobrunner.models import Task
from jobrunner.schema import TaskType
from tests.conftest import get_trace
from tests.factories import ensure_docker_images_present


log = logging.getLogger(__name__)


@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_integration(
    tmp_work_dir,
    docker_cleanup,
    requests_mock,
    monkeypatch,
    test_repo,
):
    api = get_executor_api()

    monkeypatch.setattr("jobrunner.config.controller.USING_DUMMY_DATA_BACKEND", True)
    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_ENDPOINT", "http://testserver/api/v2/"
    )
    # Disable repo URL checking so we can run using a local test repo
    monkeypatch.setattr("jobrunner.config.controller.ALLOWED_GITHUB_ORGS", None)
    # Ensure that we have enough workers to start the jobs we expect in the test
    # (CI may have fewer actual available workers than this)
    monkeypatch.setattr("jobrunner.config.controller.MAX_WORKERS", 4)

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
            "branch": "HEAD",
        },
        "codelists_ok": True,
        "database_name": "dummy",
        "sha": test_repo.commit,
        "created_by": "user",
        "project": "project",
        "orgs": ["org"],
        "backend": "test",
    }
    requests_mock.get(
        "http://testserver/api/v2/job-requests/?backend=expectations",
        json={
            "results": [job_request_1],
        },
    )
    requests_mock.post("http://testserver/api/v2/jobs/", json={})

    # Run sync to grab the JobRequest from the mocked job-server
    jobrunner.sync.sync()
    # Check that expected number of pending jobs are created
    jobs = get_posted_jobs(requests_mock)
    assert [job["status"] for job in jobs.values()] == ["pending"] * 7, list(
        jobs.values()
    )[0]["status_message"]

    # no active tasks yet
    assert not get_active_db_tasks()

    # Execute one tick of the controller run loop and then sync
    # The controller creates one runjob task, for the one action that has no
    # dependencies, and marks that job as running
    jobrunner.controller.main.handle_jobs()
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 1
    assert active_tasks[0].type == TaskType.RUNJOB
    assert active_tasks[0].id.startswith(jobs["generate_dataset"]["identifier"])
    # stage is None before the task has been picked up by the agent
    assert active_tasks[0].agent_stage is None

    jobrunner.sync.sync()

    def assert_generate_dataset_dependency_running(jobs):
        assert jobs["generate_dataset"]["status"] == "running"
        for action in [
            "prepare_data_m_ehrql",
            "prepare_data_f_ehrql",
            "prepare_data_with_quote_in_filename_ehrql",
            "analyse_data_ehrql",
            "test_reusable_action_ehrql",
            "test_cancellation_ehrql",
        ]:
            assert jobs[action]["status_message"].startswith("Waiting on dependencies")

    # We should now have one running job and all others waiting on dependencies
    jobs = get_posted_jobs(requests_mock)
    assert_generate_dataset_dependency_running(jobs)

    # Execute one tick of the agent run loop to pick up the runjob task
    # After one tick, the task should have moved to the PREPARED stage
    jobrunner.agent.main.handle_tasks(api)
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 1
    assert active_tasks[0].agent_stage == "prepared"

    # sync again; no change to status of jobs
    jobrunner.sync.sync()
    # still one running job and all others waiting on dependencies
    jobs = get_posted_jobs(requests_mock)
    assert_generate_dataset_dependency_running(jobs)

    # After one tick of the agent loop, the task should have moved to EXECUTING status
    jobrunner.agent.main.handle_tasks(api)
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 1
    assert active_tasks[0].agent_stage == "executing"

    # sync again; no change to status of jobs
    jobrunner.sync.sync()
    # still one running job and all others waiting on dependencies
    jobs = get_posted_jobs(requests_mock)
    assert_generate_dataset_dependency_running(jobs)

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
        "database_name": "dummy",
        "sha": test_repo.commit,
        "created_by": "user",
        "project": "project",
        "orgs": ["org"],
        "backend": "test",
    }
    requests_mock.get(
        "http://testserver/api/v2/job-requests/?backend=expectations",
        json={
            "results": [job_request_1, job_request_2],
        },
    )
    jobrunner.sync.sync()

    # Execute one tick of the controller run loop again to pick up the
    # cancelled job and the second job request and then sync
    # We now have 2 RUNJOB tasks (for generate_dataset, which is still executing,
    # and the new generate_dataset_with_dummy_data, which has no dependencies)
    # The cancelled job is now marked as cancelled, but no CANCELJOB task is created,
    # because no RUNJOB task had been created for it
    # The others are waiting on dependencies, so no tasks have been created for them yet
    jobrunner.controller.main.handle_jobs()
    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 2
    assert active_tasks[0].agent_stage == "executing"
    # new task has no stage as it hasb't been picked up by agent yet
    assert active_tasks[1].agent_stage is None

    # sync to confirm updated jobs have been posted back to job-server
    jobrunner.sync.sync()
    jobs = get_posted_jobs(requests_mock)
    assert jobs["generate_dataset"]["status"] == "running"
    # The new action does not depend on generate_dataset
    assert jobs["generate_dataset_with_dummy_data"]["status"] == "running"
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

    # Run the agent loop until there are no active tasks left; the generate_dataset jobs should be done
    jobrunner.agent.main.main(exit_callback=lambda active_tasks: len(active_tasks) == 0)

    # Run the controller again, this should:
    # - pick up the completed task and mark generate_dataset as succeeded
    # - add RUNJOB tasks for the 4 jobs that depend on generate_dataset and set the Job state to running
    # - the analyse_data job is still pending
    jobrunner.controller.main.handle_jobs()
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

    jobrunner.sync.sync()
    jobs = get_posted_jobs(requests_mock)
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

    # Run the agent loop until there are no active tasks left; the 4 running jobs
    # are now done
    jobrunner.agent.main.main(exit_callback=lambda active_tasks: len(active_tasks) == 0)
    # Run the controller again, this should find the 4 jobs it thinks are still running,
    # identify that their tasks are completed, and mark them as succeeded
    # And it will start a new task for the analyse_data action now that its
    # dependencies have succeeded
    jobrunner.controller.main.handle_jobs()

    active_tasks = get_active_db_tasks()
    assert len(active_tasks) == 1

    jobrunner.sync.sync()
    jobs = get_posted_jobs(requests_mock)
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
    jobrunner.agent.main.main(exit_callback=lambda active_tasks: len(active_tasks) == 0)
    jobrunner.controller.main.handle_jobs()

    # no tasks left to do
    active_tasks = get_active_db_tasks()
    assert not len(active_tasks)

    jobrunner.sync.sync()
    jobs = get_posted_jobs(requests_mock)
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
    executed_jobs = [s for s in job_spans if "exit_code" in s.attributes]
    assert len(executed_jobs) == 7
    assert sum(s.attributes["exit_code"] for s in executed_jobs) == 0

    # If this fails, it might be that your docker images have missing labels,
    # try pulling.  If that fails, it maybe the latest images are missing
    # labels.
    assert not any(s.attributes["action_created"] == "unknown" for s in executed_jobs)

    loop_spans = [s for s in get_trace("agent_loop") if s.name == "AGENT_LOOP"]
    assert len(loop_spans) > 1


def get_posted_jobs(requests_mock):
    data = requests_mock.last_request.json()
    return {job["action"]: job for job in data}


def get_active_db_tasks():
    return find_where(Task, active=True)
