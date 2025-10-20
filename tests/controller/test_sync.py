from controller import sync
from controller.models import JobRequest


def test_job_request_from_remote_format():
    remote_job_request = {
        "identifier": "123",
        "workspace": {
            "name": "testing",
            "repo": "https://github.com/opensafely/foo",
            "branch": "master",
        },
        "database_name": "default",
        "requested_actions": ["generate_cohort"],
        "cancelled_actions": ["analyse"],
        "force_run_dependencies": True,
        "sha": "abcdef",
        "codelists_ok": True,
        "created_by": "user",
        "project": "project",
        "orgs": ["org"],
        "backend": "test",
    }
    expected = JobRequest(
        id="123",
        repo_url="https://github.com/opensafely/foo",
        commit="abcdef",
        branch="master",
        workspace="testing",
        codelists_ok=True,
        database_name="default",
        requested_actions=["generate_cohort"],
        cancelled_actions=["analyse"],
        force_run_dependencies=True,
        backend="test",
        original=remote_job_request,
    )
    job_request = sync.job_request_from_remote_format(remote_job_request)
    assert job_request == expected
