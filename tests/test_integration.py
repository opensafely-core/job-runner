import os
import tempfile

import pytest

from jobrunner.job import Job

job_spec = {
    "url": "http://localhost:8000/jobs/8/",
    "backend": "tpp",
    "operation": "do_thing",
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


@pytest.fixture(scope="function")
def mock_env(monkeypatch):
    storage_root = tempfile.TemporaryDirectory().name
    high_privacy_storage_base = os.path.join(storage_root, "highsecurity")
    medium_privacy_storage_base = os.path.join(storage_root, "mediumsecurity")
    os.makedirs(high_privacy_storage_base)
    os.makedirs(medium_privacy_storage_base)
    monkeypatch.setenv("BACKEND", "tpp")
    monkeypatch.setenv("HIGH_PRIVACY_STORAGE_BASE", high_privacy_storage_base)
    monkeypatch.setenv("MEDIUM_PRIVACY_STORAGE_BASE", medium_privacy_storage_base)


@pytest.mark.skip(
    reason="Currently breaks other tests; also requires docker images to be pulled"
)
def test_local_run(mock_env):
    job = Job(job_spec)
    result = job.main(run_locally=True)
    assert result["status_message"] == "Fresh output generated"
    with open(result["output_locations"][0]["location"], "r") as f:
        result = f.read()
        assert "(16 vars, 1,000 obs)" in result
