from jobrunner.models import JobRequest
from jobrunner.sync import job_request_from_remote_format


def test_job_request_from_remote_format():
    remote_job_request = {
        "identifier": "123",
        "workspace": {
            "name": "testing",
            "repo": "https://github.com/opensafely/foo",
            "branch": "master",
            "db": "full",
        },
        "requested_actions": ["generate_cohort"],
        "cancelled_actions": ["analyse"],
        "force_run_dependencies": True,
        "sha": "abcdef",
    }
    expected = JobRequest(
        id="123",
        repo_url="https://github.com/opensafely/foo",
        commit="abcdef",
        branch="master",
        workspace="testing",
        database_name="full",
        requested_actions=["generate_cohort"],
        cancelled_actions=["analyse"],
        force_run_dependencies=True,
        original=remote_job_request,
    )
    job_request = job_request_from_remote_format(remote_job_request)
    assert job_request == expected
