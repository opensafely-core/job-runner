"""
Big integration tests that create a basic project in a git repo, mocks out a
JobRequest from the job-server to run it, and then exercises the sync and run
loops to run entire pipeline
"""
import json
import logging

import pytest

import jobrunner.run
import jobrunner.sync
from jobrunner.executors import get_executor_api
from tests.factories import ensure_docker_images_present


log = logging.getLogger(__name__)


@pytest.mark.parametrize("executor_api", [True, False])
@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_integration(
    executor_api, tmp_work_dir, docker_cleanup, requests_mock, monkeypatch, test_repo
):
    # TODO: add the following parametrize decorator back to this test:
    #
    #   @pytest.mark.parametrize("extraction_tool", ["cohortextractor", "databuilder"])
    #
    # Databuilder currently supports too few options in dummy data (at the time
    # of writing we are still building out the "walking skeleton") to be run
    # alongside cohortextractor in this test, however once it supports a close
    # enough set of dummy data we can merge them into a single test.
    extraction_tool = "cohortextractor"

    if extraction_tool == "cohortextractor":
        generate_action = "generate_cohort"
    else:
        generate_action = "generate_dataset"

    monkeypatch.setattr("jobrunner.config.EXECUTION_API", executor_api)
    if executor_api:
        api = get_executor_api()
    else:
        api = None

    monkeypatch.setattr(
        "jobrunner.config.JOB_SERVER_ENDPOINT", "http://testserver/api/v2/"
    )
    # Disable repo URL checking so we can run using a local test repo
    monkeypatch.setattr("jobrunner.config.ALLOWED_GITHUB_ORGS", None)
    # Make job execution order deterministic
    monkeypatch.setattr("jobrunner.config.RANDOMISE_JOB_ORDER", False)

    if extraction_tool == "cohortextractor":
        image = "cohortextractor"
    else:
        image = "databuilder:v0.36.0"
    ensure_docker_images_present(image, "python")

    # Set up a mock job-server with a single job request
    job_request_1 = {
        "identifier": 1,
        "requested_actions": [
            f"analyse_data_{extraction_tool}",
            f"test_reusable_action_{extraction_tool}",
            f"test_cancellation_{extraction_tool}",
        ],
        "cancelled_actions": [],
        "force_run_dependencies": False,
        "workspace": {
            "name": "testing",
            "repo": str(test_repo.path),
            "branch": "HEAD",
            "db": "dummy",
        },
        "sha": test_repo.commit,
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
    assert [job["status"] for job in jobs.values()] == ["pending"] * 7
    # Execute one tick of the run loop and then sync
    jobrunner.run.handle_jobs(api)
    jobrunner.sync.sync()
    # We should now have one running job and all others waiting on dependencies
    jobs = get_posted_jobs(requests_mock)
    assert jobs[generate_action]["status"] == "running"
    for action in [
        f"prepare_data_m_{extraction_tool}",
        f"prepare_data_f_{extraction_tool}",
        f"prepare_data_with_quote_in_filename_{extraction_tool}",
        f"analyse_data_{extraction_tool}",
        f"test_reusable_action_{extraction_tool}",
        f"test_cancellation_{extraction_tool}",
    ]:
        assert jobs[action]["status_message"].startswith("Waiting on dependencies")

    # Update the existing job request to mark a job as cancelled, add a new job
    # request to be run and then sync
    job_request_1["cancelled_actions"] = [f"test_cancellation_{extraction_tool}"]
    job_request_2 = {
        "identifier": 2,
        "requested_actions": [
            "generate_cohort_with_dummy_data",
        ],
        "cancelled_actions": [],
        "force_run_dependencies": False,
        "workspace": {
            "name": "testing",
            "repo": str(test_repo.path),
            "branch": "HEAD",
            "db": "dummy",
        },
        "sha": test_repo.commit,
    }
    requests_mock.get(
        "http://testserver/api/v2/job-requests/?backend=expectations",
        json={
            "results": [job_request_1, job_request_2],
        },
    )
    jobrunner.sync.sync()

    # Run the main loop until there are no jobs left and then sync
    jobrunner.run.main(exit_callback=lambda active_jobs: len(active_jobs) == 0)
    jobrunner.sync.sync()

    # All jobs should now have succeeded apart from the cancelled one
    jobs = get_posted_jobs(requests_mock)
    assert jobs[generate_action]["status"] == "succeeded"
    assert jobs[f"prepare_data_m_{extraction_tool}"]["status"] == "succeeded"
    assert jobs[f"prepare_data_f_{extraction_tool}"]["status"] == "succeeded"
    assert (
        jobs[f"prepare_data_with_quote_in_filename_{extraction_tool}"]["status"]
        == "succeeded"
    )
    assert jobs[f"analyse_data_{extraction_tool}"]["status"] == "succeeded"
    assert jobs[f"test_reusable_action_{extraction_tool}"]["status"] == "succeeded"
    assert jobs[f"test_cancellation_{extraction_tool}"]["status"] == "failed"

    high_privacy_workspace = tmp_work_dir / "high_privacy_workspaces_dir" / "testing"
    medium_privacy_workspace = (
        tmp_work_dir / "medium_privacy_workspaces_dir" / "testing"
    )

    # Check that the manifest contains what we expect. This is a subset of what used to be in the manifest, to support
    # nicer UX for osrelease. See the comment in manage_jobs.finalize_job().
    manifest_file = medium_privacy_workspace / "metadata" / "manifest.json"
    manifest = json.load(manifest_file.open())
    assert manifest["workspace"] == "testing"
    assert manifest["repo"] == str(test_repo.path)

    if extraction_tool == "cohortextractor":
        output_name = "input"
    else:
        output_name = "dataset"

    # Check that all the outputs have been produced
    for highly_sensitive_output in [
        f"output/{output_name}.csv",  # the cohort/dataset
        "output/extra/input.csv",  # extracted from dummy data
        f"{extraction_tool}-male.csv",  # intermediate analysis
        f"{extraction_tool}-female.csv",  # intermediate analysis
        f"{extraction_tool}-qu'ote.csv",  # checking handling of problematic characters in filenames
        f"output/{output_name}.backup.csv",  # from the reusable action
    ]:
        path = high_privacy_workspace / highly_sensitive_output
        assert path.exists(), highly_sensitive_output

    for moderately_sensitive_output in [
        f"{extraction_tool}-counts.txt",  # the study's actual output
    ]:
        assert (medium_privacy_workspace / moderately_sensitive_output).exists()

    # Check that we don't produce outputs for cancelled jobs
    assert not (high_privacy_workspace / f"{extraction_tool}-somefile.csv").exists()


@pytest.mark.parametrize("executor_api", [True, False])
@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_integration_with_databuilder(
    executor_api, tmp_work_dir, docker_cleanup, requests_mock, monkeypatch, test_repo
):
    # TODO: merge this test into test_integration
    #
    # Databuilder currently supports too few options in dummy data (at the time
    # of writing we are still building out the "walking skeleton") to be run
    # alongside cohortextractor in this test, however once it supports a close
    # enough set of dummy data we can merge them into a single test.
    extraction_tool = "databuilder"

    monkeypatch.setattr("jobrunner.config.EXECUTION_API", executor_api)
    if executor_api:
        api = get_executor_api()
    else:
        api = None

    monkeypatch.setattr(
        "jobrunner.config.JOB_SERVER_ENDPOINT", "http://testserver/api/v2/"
    )
    # Disable repo URL checking so we can run using a local test repo
    monkeypatch.setattr("jobrunner.config.ALLOWED_GITHUB_ORGS", None)
    # Make job execution order deterministic
    monkeypatch.setattr("jobrunner.config.RANDOMISE_JOB_ORDER", False)

    ensure_docker_images_present("databuilder:v0.36.0", "python")

    # Set up a mock job-server with a single job request
    job_request_1 = {
        "identifier": 1,
        "requested_actions": [
            f"analyse_data_{extraction_tool}",
            f"test_cancellation_{extraction_tool}",
        ],
        "cancelled_actions": [],
        "force_run_dependencies": False,
        "workspace": {
            "name": "testing",
            "repo": str(test_repo.path),
            "branch": "HEAD",
            "db": "dummy",
        },
        "sha": test_repo.commit,
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
    assert [job["status"] for job in jobs.values()] == ["pending"] * 3, list(
        jobs.values()
    )[0]["status_message"]
    # Execute one tick of the run loop and then sync
    jobrunner.run.handle_jobs(api)
    jobrunner.sync.sync()
    # We should now have one running job and all others waiting on dependencies
    jobs = get_posted_jobs(requests_mock)
    assert jobs["generate_dataset"]["status"] == "running"
    for action in [
        f"analyse_data_{extraction_tool}",
        f"test_cancellation_{extraction_tool}",
    ]:
        assert jobs[action]["status_message"].startswith("Waiting on dependencies")

    # Update the existing job request to mark a job as cancelled, add a new job
    # request to be run and then sync
    job_request_1["cancelled_actions"] = [f"test_cancellation_{extraction_tool}"]
    job_request_2 = {
        "identifier": 2,
        "requested_actions": [
            "derp_action",
        ],
        "cancelled_actions": [],
        "force_run_dependencies": False,
        "workspace": {
            "name": "testing",
            "repo": str(test_repo.path),
            "branch": "HEAD",
            "db": "dummy",
        },
        "sha": test_repo.commit,
    }
    requests_mock.get(
        "http://testserver/api/v2/job-requests/?backend=expectations",
        json={
            "results": [job_request_1, job_request_2],
        },
    )
    jobrunner.sync.sync()

    # Run the main loop until there are no jobs left and then sync
    jobrunner.run.main(exit_callback=lambda active_jobs: len(active_jobs) == 0)
    jobrunner.sync.sync()

    # All jobs should now have succeeded apart from the cancelled one
    jobs = get_posted_jobs(requests_mock)
    test_cancellation_job = jobs.pop(f"test_cancellation_{extraction_tool}")
    for action, job in jobs.items():
        assert (
            job["status"] == "succeeded"
        ), f"{action} failed with: {job['status_message']}"

    assert test_cancellation_job["status"] == "failed"

    high_privacy_workspace = tmp_work_dir / "high_privacy_workspaces_dir" / "testing"
    medium_privacy_workspace = (
        tmp_work_dir / "medium_privacy_workspaces_dir" / "testing"
    )

    # Check that the manifest contains what we expect. This is a subset of what used to be in the manifest, to support
    # nicer UX for osrelease. See the comment in manage_jobs.finalize_job().
    manifest_file = medium_privacy_workspace / "metadata" / "manifest.json"
    manifest = json.load(manifest_file.open())
    assert manifest["workspace"] == "testing"
    assert manifest["repo"] == str(test_repo.path)

    # Check that all the outputs have been produced
    assert (high_privacy_workspace / "output/dataset.csv").exists()
    assert (medium_privacy_workspace / "output/count_by_year.csv").exists()

    # Check that we don't produce outputs for cancelled jobs
    assert not (high_privacy_workspace / "output/count_by_year_cancelled.csv").exists()


def get_posted_jobs(requests_mock):
    data = requests_mock.last_request.json()
    return {job["action"]: job for job in data}
