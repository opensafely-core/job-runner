from unittest.mock import patch

import pytest
import requests_mock

from jobrunner.main import watch
from tests.common import BrokenJob, SlowJob, WorkingJob, test_job_list


@pytest.fixture(scope="function")
def mock_env(monkeypatch):
    monkeypatch.setenv("BACKEND", "tpp")
    monkeypatch.setenv("HIGH_PRIVACY_STORAGE_BASE", "/tmp/storage/highsecurity")
    monkeypatch.setenv("MEDIUM_PRIVACY_STORAGE_BASE", "/tmp/storage/mediumsecurity")
    monkeypatch.setenv("JOB_SERVER_ENDPOINT", "http://test.com/jobs/")


def test_watch_broken_job(mock_env):
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list())
        adapter = m.patch("/jobs/0/")
        watch("http://test.com/jobs/", loop=False, job_class=BrokenJob)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {
            "status_code": 99,
            "status_message": "Unclassified error id BrokenJob",
        }


def test_watch_working_job(mock_env):
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list())
        adapter = m.patch("/jobs/0/")
        watch("http://test.com/jobs/", loop=False, job_class=WorkingJob)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {
            "outputs": [],
            "status_code": 0,
            "status_message": "",
        }


@patch("runner.main.HOUR", 0.001)
def test_watch_timeout_job(mock_env):
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list())
        adapter = m.patch("/jobs/0/")
        watch("http://test.com/jobs/", loop=False, job_class=SlowJob)
        assert adapter.request_history[0].json()["started"] is True
        assert adapter.request_history[1].json() == {
            "status_code": -1,
            "status_message": "TimeoutError(86400s) id SlowJob",
        }
