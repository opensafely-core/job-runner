import requests_mock
import runner
from unittest.mock import patch
import time


def test_job():
    return {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "url": "http://test.com/jobs/0/",
                "repo": "myrepo",
                "tag": "mytag",
                "started": False,
                "operation": "generate_cohort",
                "status_code": None,
                "output_url": None,
                "created_at": None,
                "started_at": None,
                "completed_at": None,
            },
        ],
    }


def dummy_broken_job(job):
    raise KeyError()


def dummy_working_job(job, sleep=False):
    job["output_url"] = "output_url"
    if sleep:
        time.sleep(1)
    return job


def dummy_slow_job(job):
    return dummy_working_job(job, sleep=True)


@patch("runner.run_job", dummy_broken_job)
def test_watch_broken_job():
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job())
        adapter = m.patch("/jobs/0/")
        runner.watch("http://test.com/jobs/", loop=False)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {"status_code": 1}


@patch("runner.run_job", dummy_working_job)
def test_watch_working_job():
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job())
        adapter = m.patch("/jobs/0/")
        runner.watch("http://test.com/jobs/", loop=False)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {
            "output_url": "output_url",
            "status_code": 0,
        }


@patch("runner.run_job", dummy_slow_job)
@patch("runner.HOUR", 0.001)
def test_watch_timeout_job():
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job())
        adapter = m.patch("/jobs/0/")
        runner.watch("http://test.com/jobs/", loop=False)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {"status_code": -1}
