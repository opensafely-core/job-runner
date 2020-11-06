from jobrunner.sync import job_request_from_remote_format
from jobrunner.models import JobRequest


def test_job_request_from_remote_format():
    remote_job_request = {
        "pk": "123",
        "workspace": {
            "repo": "https://github.com/opensafely/foo",
            "branch": "master",
            "db": "full",
        },
        "workspace_id": "5",
        "action_id": "generate_cohort",
        "force_run": True,
        "force_run_dependencies": True,
    }
    expected = JobRequest(
        id="123",
        repo_url="https://github.com/opensafely/foo",
        commit=None,
        branch="master",
        workspace="foo-5",
        database_name="full",
        action="generate_cohort",
        force_run=True,
        force_run_dependencies=True,
        original=remote_job_request,
    )
    job_request = job_request_from_remote_format(remote_job_request)
    assert job_request == expected
