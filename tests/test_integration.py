import pytest

from jobrunner.job import Job

job_spec = {
    "url": "http://localhost:8000/jobs/8/",
    "backend": "tpp",
    "action_id": "do_thing",
    "workspace": {
        "id": 3,
        "url": "http://localhost:8000/workspaces/3/",
        "name": "my workspace",
        "repo": "https://github.com/opensafely/job-integration-tests",
        "branch": "master",
        "db": "dummy",
        "owner": "me",
    },
    "workspace_id": 3,
}


@pytest.mark.skip(
    reason="Currently breaks other tests; also requires docker images to be pulled"
)
def test_local_run():
    job = Job(job_spec)
    result = job.main(run_locally=True)
    assert result["status_message"] == "Fresh output generated"
    with open(result["output_locations"][0]["location"], "r") as f:
        result = f.read()
        assert "(16 vars, 1,000 obs)" in result
