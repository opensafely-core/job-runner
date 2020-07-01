import requests_mock
import runner
from unittest.mock import patch
import time

from runner import make_volume_name
from runner import run_cohort_extractor
from runner.exceptions import BadDockerImageName

import pytest


@pytest.fixture(scope="function")
def mock_env(monkeypatch):
    monkeypatch.setenv("BACKEND", "tpp")


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
                "backend": "tpp",
                "db": "full",
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


@patch("runner.run_cohort_extractor", dummy_broken_job)
def test_watch_broken_job(mock_env):
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job())
        adapter = m.patch("/jobs/0/")
        runner.watch("http://test.com/jobs/", loop=False)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {"status_code": 1}


@patch("runner.run_cohort_extractor", dummy_working_job)
def test_watch_working_job(mock_env):
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job())
        adapter = m.patch("/jobs/0/")
        runner.watch("http://test.com/jobs/", loop=False)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {
            "output_url": "output_url",
            "status_code": 0,
        }


@patch("runner.run_cohort_extractor", dummy_slow_job)
@patch("runner.HOUR", 0.001)
def test_watch_timeout_job(mock_env):
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job())
        adapter = m.patch("/jobs/0/")
        runner.watch("http://test.com/jobs/", loop=False)
        assert adapter.request_history[0].json() == {"started": True}
        assert adapter.request_history[1].json() == {"status_code": -1}


def test_make_volume_name():
    repo = "https://github.com/opensafely/hiv-research/"
    branch = "feasibility-no"
    db_flavour = "full"
    assert (
        make_volume_name(repo, branch, db_flavour) == "hiv-research-feasibility-no-full"
    )


def test_bad_volume_name_raises():
    bad_name = "-badname"
    with pytest.raises(BadDockerImageName) as e:
        run_cohort_extractor(
            {"repo": bad_name, "tag": "thing", "db": "FULL", "url": ""}
        )
    assert e.value.args == (f"Bad image name {bad_name}",)
