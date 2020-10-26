from jobrunner.sync import job_request_from_remote_format


def test_job_request_from_remote_format():
    remote_job_request = {
        "pk": "123",
        "workspace": {"repo": "https://github.com/opensafely/foo", "branch": "master"},
        "workspace_id": "5",
        "action_id": "generate_cohort",
    }
    expected = {
        "id": "123",
        "repo_url": "https://github.com/opensafely/foo",
        "commit": None,
        "branch": "master",
        "workspace": "5",
        "action": "generate_cohort",
        "original": remote_job_request,
    }
    job_request = job_request_from_remote_format(remote_job_request)
    assert job_request == expected
