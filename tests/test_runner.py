import requests_mock
import runner
from unittest.mock import patch
import time

from runner import make_volume_name
from runner import make_container_name
from runner.exceptions import CohortExtractorError
from runner.exceptions import OpenSafelyError
from runner.exceptions import RepoNotFound

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
        assert adapter.request_history[1].json() == {
            "status_code": 99,
            "status_message": "Unclassified error id job#0",
        }


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
        assert adapter.request_history[0].json()["started"] is True
        assert adapter.request_history[1].json() == {
            "status_code": -1,
            "status_message": "TimeoutError(86400s) id job#0",
        }


def test_make_volume_name():
    repo = "https://github.com/opensafely/hiv-research/"
    branch = "feasibility-no"
    db_flavour = "full"
    assert (
        make_volume_name(repo, branch, db_flavour) == "hiv-research-feasibility-no-full"
    )


def test_bad_volume_name_raises():
    bad_name = "/badname"
    assert make_container_name(bad_name) == "badname"


def test_docker_exception():
    error = CohortExtractorError("thing not to leak")
    assert (
        error.safe_details()
        == "CohortExtractorError: [possibly-unsafe details redacted]"
    )
    assert repr(error) == "CohortExtractorError('thing not to leak')"


def test_opensafely_exception():
    error = RepoNotFound("thing OK to leak")
    assert error.safe_details() == "RepoNotFound: thing OK to leak"
    assert repr(error) == "RepoNotFound('thing OK to leak')"


def test_reserved_exception():
    class InvalidError(OpenSafelyError):
        status_code = -1

    with pytest.raises(AssertionError) as e:
        raise InvalidError()
    assert "reserved" in e.value.args[0]

    with pytest.raises(RepoNotFound):
        raise RepoNotFound()
