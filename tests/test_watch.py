from unittest.mock import patch

import requests_mock

from jobrunner.main import watch
from tests.common import BrokenJob, SlowJob, WorkingJob, test_job_list


def test_watch_broken_job():
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list())
        adapter = m.patch("/jobs/0/")
        watch("http://test.com/jobs/", loop=False, job_class=BrokenJob)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {
            "status_code": 99,
            "status_message": "Unclassified error id BrokenJob",
        }


def test_watch_working_job():
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


@patch("jobrunner.main.HOUR", 0.001)
def test_watch_timeout_job():
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list())
        adapter = m.patch("/jobs/0/")
        watch("http://test.com/jobs/", loop=False, job_class=SlowJob)
        assert adapter.request_history[0].json()["started"] is True
        assert adapter.request_history[1].json() == {
            "status_code": -1,
            "status_message": "TimeoutError(86400s) id SlowJob",
        }
