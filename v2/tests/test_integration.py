from pathlib import Path
import subprocess

import jobrunner.sync
import jobrunner.run
from jobrunner import config, docker


# Big integration test that creates a basic project in a git repo, mocks out a
# JobRequest from the job-server to run it, and then exercises the sync and run
# loops to run entire pipeline
def test_integration(tmp_work_dir, docker_cleanup, requests_mock):
    ensure_docker_images_present()
    project_fixture = str(Path(__file__).parent.resolve() / "fixtures/full_project")
    repo_path = tmp_work_dir / "test-repo"
    commit_directory_contents(repo_path, project_fixture)
    requests_mock.get(
        "https://jobs.opensafely.org/api/job-requests?active=true&backend=expectations",
        json=[
            {
                "pk": 1,
                "action_id": "analyse_data",
                "force_run": False,
                "force_run_dependencies": False,
                "workspace_id": 1,
                "workspace": {
                    "repo": str(repo_path),
                    "branch": "master",
                    "db": "dummy",
                },
            }
        ],
    )
    requests_mock.post(
        "https://jobs.opensafely.org/api/jobs", json={},
    )
    # Run sync to grab the JobRequest from the mocked job-server
    jobrunner.sync.sync()
    # Check that three pending jobs are created
    jobs = get_posted_jobs(requests_mock)
    assert [job["status"] for job in jobs.values()] == ["P", "P", "P", "P"]
    # Exectue one tick of the run loop and then sync
    jobrunner.run.handle_jobs()
    jobrunner.sync.sync()
    # We should now have one running job and two waiting on dependencies
    jobs = get_posted_jobs(requests_mock)
    assert jobs["generate_cohort"]["status"] == "R"
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
    # All jobs should now be completed
    jobs = get_posted_jobs(requests_mock)
    assert jobs["generate_cohort"]["status"] == "C"
    assert jobs["prepare_data_m"]["status"] == "C"
    assert jobs["prepare_data_f"]["status"] == "C"
    assert jobs["analyse_data"]["status"] == "C"


def commit_directory_contents(repo_path, directory):
    env = {"GIT_WORK_TREE": directory, "GIT_DIR": repo_path}
    subprocess.run(["git", "init", "--bare", "--quiet", repo_path], check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"], check=True, env=env
    )
    subprocess.run(["git", "add", "."], check=True, env=env)
    subprocess.run(["git", "commit", "--quiet", "-m", "initial"], check=True, env=env)


def ensure_docker_images_present():
    for image in ["cohortextractor", "jupyter"]:
        full_image = f"{config.DOCKER_REGISTRY}/{image}"
        if not docker.image_exists_locally(full_image):
            subprocess.run(["docker", "pull", "--quiet", full_image], check=True)


def get_posted_jobs(requests_mock):
    data = requests_mock.last_request.json()
    return {job["action"]: job for job in data}
