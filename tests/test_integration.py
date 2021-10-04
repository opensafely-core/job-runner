import json
import logging
from pathlib import Path

import pytest

import jobrunner.run
import jobrunner.sync
from jobrunner import config
from jobrunner.lib import docker
from jobrunner.lib.subprocess_utils import subprocess_run

log = logging.getLogger(__name__)


# Big integration test that creates a basic project in a git repo, mocks out a
# JobRequest from the job-server to run it, and then exercises the sync and run
# loops to run entire pipeline
@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_integration(tmp_work_dir, docker_cleanup, requests_mock, monkeypatch):
    monkeypatch.setattr(
        "jobrunner.config.JOB_SERVER_ENDPOINT", "http://testserver/api/v2/"
    )
    # Disable repo URL checking so we can run using a local test repo
    monkeypatch.setattr("jobrunner.config.ALLOWED_GITHUB_ORGS", None)
    # Make job execution order deterministic
    monkeypatch.setattr("jobrunner.config.RANDOMISE_JOB_ORDER", False)
    ensure_docker_images_present("cohortextractor", "python")

    # Take our test project fixture and commit it to a temporary git repo
    project_fixture = str(Path(__file__).parent.resolve() / "fixtures/full_project")
    repo_path = tmp_work_dir / "test-repo"
    commit = commit_directory_contents(repo_path, project_fixture)

    # Set up a mock job-server with a single job request
    job_request_1 = {
        "identifier": 1,
        "requested_actions": [
            "analyse_data",
            "test_reusable_action",
            "test_cancellation",
        ],
        "cancelled_actions": [],
        "force_run_dependencies": False,
        "workspace": {
            "name": "testing",
            "repo": str(repo_path),
            "branch": "HEAD",
            "db": "dummy",
        },
        "sha": commit,
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
    jobrunner.run.handle_jobs()
    jobrunner.sync.sync()
    # We should now have one running job and all others waiting on dependencies
    jobs = get_posted_jobs(requests_mock)
    assert jobs["generate_cohort"]["status"] == "running"
    for action in [
        "prepare_data_m",
        "prepare_data_f",
        "prepare_data_with_quote_in_filename",
        "analyse_data",
        "test_reusable_action",
        "test_cancellation",
    ]:
        assert jobs[action]["status_message"].startswith("Waiting on dependencies")

    # Update the existing job request to mark a job as cancelled, add a new job
    # request to be run and then sync
    job_request_1["cancelled_actions"] = ["test_cancellation"]
    job_request_2 = {
        "identifier": 2,
        "requested_actions": [
            "generate_cohort_with_dummy_data",
        ],
        "cancelled_actions": [],
        "force_run_dependencies": False,
        "workspace": {
            "name": "testing",
            "repo": str(repo_path),
            "branch": "HEAD",
            "db": "dummy",
        },
        "sha": commit,
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
    assert jobs["generate_cohort"]["status"] == "succeeded"
    assert jobs["generate_cohort_with_dummy_data"]["status"] == "succeeded"
    assert jobs["prepare_data_m"]["status"] == "succeeded"
    assert jobs["prepare_data_f"]["status"] == "succeeded"
    assert jobs["prepare_data_with_quote_in_filename"]["status"] == "succeeded"
    assert jobs["analyse_data"]["status"] == "succeeded"
    assert jobs["test_reusable_action"]["status"] == "succeeded"
    assert jobs["test_cancellation"]["status"] == "failed"

    high_privacy_workspace = tmp_work_dir / "high_privacy_workspaces_dir" / "testing"
    medium_privacy_workspace = (
        tmp_work_dir / "medium_privacy_workspaces_dir" / "testing"
    )

    # Check that the manifest contains what we expect. This is a subset of what used to be in the manifest, to support
    # nicer UX for osrelease. See the comment in manage_jobs.finalize_job().
    manifest_file = medium_privacy_workspace / "metadata" / "manifest.json"
    manifest = json.load(manifest_file.open())
    assert manifest["workspace"] == "testing"
    assert manifest["repo"] == str(repo_path)

    # Check that all the outputs have been produced
    for highly_sensitive_output in [
        "output/input.csv",  # the cohort
        "output/extra/input.csv",  # extracted from dummy data
        "male.csv",  # intermediate analysis
        "female.csv",  # intermediate analysis
        "qu'ote.csv",  # checking handling of problematic characters in filenames
        "output/input.backup.csv",  # from the reusable action
    ]:
        assert (high_privacy_workspace / highly_sensitive_output).exists()

    for moderately_sensitive_output in [
        "counts.txt",  # the study's actual output
    ]:
        assert (medium_privacy_workspace / moderately_sensitive_output).exists()

    # Check that we don't produce outputs for cancelled jobs
    assert not (high_privacy_workspace / "somefile.csv").exists()


def commit_directory_contents(repo_path, directory):
    env = {
        "GIT_WORK_TREE": directory,
        "GIT_DIR": repo_path,
        "GIT_CONFIG_GLOBAL": "/dev/null",
    }
    subprocess_run(["git", "init", "--bare", "--quiet", repo_path], check=True)
    subprocess_run(
        ["git", "config", "user.email", "test@example.com"], check=True, env=env
    )
    subprocess_run(["git", "config", "user.name", "Test"], check=True, env=env)
    subprocess_run(["git", "add", "."], check=True, env=env)
    subprocess_run(["git", "commit", "--quiet", "-m", "initial"], check=True, env=env)
    response = subprocess_run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        env=env,
        capture_output=True,
        text=True,
    )
    return response.stdout.strip()


def ensure_docker_images_present(*images):
    for image in images:
        full_image = f"{config.DOCKER_REGISTRY}/{image}"
        if not docker.image_exists_locally(full_image):
            log.info(f"Pulling Docker image {full_image}")
            subprocess_run(["docker", "pull", "--quiet", full_image], check=True)


def get_posted_jobs(requests_mock):
    data = requests_mock.last_request.json()
    return {job["action"]: job for job in data}
