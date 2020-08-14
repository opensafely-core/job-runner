import os
import tempfile
from unittest.mock import patch

import pytest
import requests_mock

from runner.exceptions import DependencyNotFinished, OpenSafelyError, RepoNotFound
from runner.job import Job
from runner.main import watch
from runner.project import parse_project_yaml
from tests.common import BrokenJob, SlowJob, WorkingJob, default_job, test_job_list


class TestError(OpenSafelyError):
    status_code = 10


@pytest.fixture(scope="function")
def mock_env(monkeypatch):
    monkeypatch.setenv("BACKEND", "tpp")
    monkeypatch.setenv("HIGH_PRIVACY_STORAGE_BASE", "/tmp/storage/highsecurity")
    monkeypatch.setenv("MEDIUM_PRIVACY_STORAGE_BASE", "/tmp/storage/mediumsecurity")
    monkeypatch.setenv("JOB_SERVER_ENDPOINT", "http://test.com/jobs/")


@pytest.fixture(scope="function")
def workspace():
    return {
        "repo": "https://github.com/repo",
        "db": "full",
        "owner": "me",
        "name": "tofu",
        "branch": "master",
        "id": 1,
    }


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


def test_exception_reporting():
    error = TestError("thing not to leak", report_args=False)
    assert error.safe_details() == "TestError: [possibly-unsafe details redacted]"
    assert repr(error) == "TestError('thing not to leak')"

    error = TestError("thing OK to leak", report_args=True)
    assert error.safe_details() == "TestError: thing OK to leak"
    assert repr(error) == "TestError('thing OK to leak')"


def test_reserved_exception():
    class InvalidError(OpenSafelyError):
        status_code = -1

    with pytest.raises(AssertionError) as e:
        raise InvalidError(report_args=True)
    assert "reserved" in e.value.args[0]

    with pytest.raises(RepoNotFound):
        raise RepoNotFound(report_args=True)


def test_never_started_dependency_exception(mock_env, workspace):
    """Does a never-run dependency mean an exception is raised and the
    dependency is kicked off?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = {"url": "", "operation": "run_model", "workspace": workspace}
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json={"results": []})
        adapter = m.post("/jobs/")
        with pytest.raises(
            DependencyNotFinished,
            match=r"Not started because dependency `generate_cohorts` has been added to the job queue",
        ):
            job = Job(job_spec, workdir=project_path)
            job.run_job_and_dependencies()

    assert adapter.request_history[0].json() == {
        "backend": "tpp",
        "callback_url": None,
        "needed_by": "run_model",
        "operation": "generate_cohorts",
        "workspace_id": workspace["id"],
    }


def test_unstarted_dependency_exception(mock_env, workspace):
    """Does a existing, but unstarted dependency mean an exception is raised?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = {
        "operation": "run_model",
        "url": "",
        "workspace": workspace,
    }
    existing_unstarted_job = default_job.copy()
    existing_unstarted_job.update(job_spec)
    existing_unstarted_job["started"] = False
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list(job=existing_unstarted_job))
        with pytest.raises(
            DependencyNotFinished,
            match=r"Not started because dependency `generate_cohorts` is waiting to start",
        ):
            job = Job(job_spec, workdir=project_path)
            job.run_job_and_dependencies()


def test_failed_dependency_exception(mock_env, workspace):
    """Does a existing, but failed dependency mean an exception is raised?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_requested = default_job.copy()
    job_requested.update({"operation": "run_model", "workspace": workspace})
    existing_failed_job = job_requested.copy()
    existing_failed_job["started"] = True
    existing_failed_job["completed_at"] = "2020-01-01"
    existing_failed_job["status_code"] = 1
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list(job=existing_failed_job))
        with pytest.raises(
            DependencyNotFinished,
            match=r"Dependency `generate_cohorts` failed, so unable to run this operation",
        ):
            job = Job(job_requested, workdir=project_path)
            job.run_job_and_dependencies()


@patch("runner.server_interaction.docker_container_exists")
def test_started_dependency_exception(mock_container_exists, mock_env, workspace):
    """Does an already-running dependency mean an exception is raised?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = {"url": "", "operation": "run_model", "workspace": workspace}
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json={"results": []})
        mock_container_exists.return_value = True
        with pytest.raises(
            DependencyNotFinished,
            match=r"Not started because dependency `generate_cohorts` is currently running",
        ):
            job = Job(job_spec, workdir=project_path)
            job.run_job_and_dependencies()


@patch("runner.utils.make_output_path")
def test_project_dependency_no_exception(dummy_output_path, mock_env, workspace):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = {
        "url": "",
        "operation": "run_model",
        "workspace": workspace,
    }
    with tempfile.TemporaryDirectory() as d:
        mock_output_filename = os.path.join(d, "input.csv")
        dummy_output_path.return_value = mock_output_filename
        with open(mock_output_filename, "w") as f:
            f.write("")
        job = Job(job_spec, workdir=project_path)
        job.run_job_and_dependencies()
