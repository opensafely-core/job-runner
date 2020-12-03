import logging
from pathlib import Path

import pytest

import jobrunner.sync
import jobrunner.run
from jobrunner import config, docker
from jobrunner.subprocess_utils import subprocess_run


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

    ensure_docker_images_present("cohortextractor", "python")
    project_fixture = str(Path(__file__).parent.resolve() / "fixtures/full_project")
    repo_path = tmp_work_dir / "test-repo"
    commit_directory_contents(repo_path, project_fixture)
    requests_mock.get(
        "http://testserver/api/v2/job-requests/?backend=expectations",
        json={
            "results": [
                {
                    "identifier": 1,
                    "requested_actions": ["analyse_data"],
                    "force_run_dependencies": False,
                    "workspace": {
                        "name": "testing",
                        "repo": str(repo_path),
                        "branch": "HEAD",
                        "db": "dummy",
                    },
                }
            ],
        },
    )
    requests_mock.post("http://testserver/api/v2/jobs/", json={})

    # Run sync to grab the JobRequest from the mocked job-server
    jobrunner.sync.sync()
    # Check that three pending jobs are created
    jobs = get_posted_jobs(requests_mock)
    assert [job["status"] for job in jobs.values()] == [
        "pending",
        "pending",
        "pending",
        "pending",
    ]
    # Exectue one tick of the run loop and then sync
    jobrunner.run.handle_jobs()
    jobrunner.sync.sync()
    # We should now have one running job and two waiting on dependencies
    jobs = get_posted_jobs(requests_mock)
    assert jobs["generate_cohort"]["status"] == "running"
    assert jobs["prepare_data_m"]["status_message"].startswith(
        "Waiting on dependencies"
    )
    assert jobs["prepare_data_f"]["status_message"].startswith(
        "Waiting on dependencies"
    )
    assert jobs["analyse_data"]["status_message"].startswith("Waiting on dependencies")
    # Run the main loop to completion and then sync
    jobrunner.run.main(exit_when_done=True)
    jobrunner.sync.sync()
    # All jobs should now have succeeded
    jobs = get_posted_jobs(requests_mock)
    assert jobs["generate_cohort"]["status"] == "succeeded"
    assert jobs["prepare_data_m"]["status"] == "succeeded"
    assert jobs["prepare_data_f"]["status"] == "succeeded"
    assert jobs["analyse_data"]["status"] == "succeeded"


def commit_directory_contents(repo_path, directory):
    env = {"GIT_WORK_TREE": directory, "GIT_DIR": repo_path}
    subprocess_run(["git", "init", "--bare", "--quiet", repo_path], check=True)
    subprocess_run(
        ["git", "config", "user.email", "test@example.com"], check=True, env=env
    )
    subprocess_run(["git", "config", "user.name", "Test"], check=True, env=env)
    subprocess_run(["git", "add", "."], check=True, env=env)
    subprocess_run(["git", "commit", "--quiet", "-m", "initial"], check=True, env=env)


def ensure_docker_images_present(*images):
    for image in images:
        full_image = f"{config.DOCKER_REGISTRY}/{image}"
        if not docker.image_exists_locally(full_image):
            log.info(f"Pulling Docker image {full_image}")
            subprocess_run(["docker", "pull", "--quiet", full_image], check=True)


def get_posted_jobs(requests_mock):
    data = requests_mock.last_request.json()
    return {job["action"]: job for job in data}
