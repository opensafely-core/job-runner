import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests_mock

from jobrunner.exceptions import DependencyNotFinished, OpenSafelyError, RepoNotFound
from jobrunner.job import Job
from jobrunner.main import watch
from jobrunner.utils import all_output_paths_for_action
from tests.common import BrokenJob, SlowJob, WorkingJob, default_job, test_job_list


class TestError(OpenSafelyError):
    status_code = 10


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


class MockSubprocess(Mock):
    @property
    def returncode(self):
        return 0


@patch("jobrunner.job.subprocess", new_callable=MockSubprocess)
def test_invoke_docker_file_copying(mock_subprocess, prepared_job_maker):
    with tempfile.TemporaryDirectory() as storage_base, tempfile.TemporaryDirectory() as workdir:
        inputs = [
            (storage_base, "inthing.csv"),
        ]
        levels_with_outputs = {"highly_sensitive": {"outthing": "outthing.csv"}}
        for base, fname in inputs:
            Path(f"{storage_base}/{fname}").touch()
        for level, outputs in levels_with_outputs.items():
            for output, path in outputs.items():
                Path(f"{workdir}/{path}").touch()

        prepared_job = prepared_job_maker(inputs=inputs, outputs=levels_with_outputs)
        job = Job(prepared_job, workdir=workdir)
        job.invoke_docker(prepared_job)

        # We expect inputs to have been copied to workdir, then
        # deleted, and outputs to have been copied to storage_base
        for base, relpath in all_output_paths_for_action(prepared_job):
            target_path = os.path.join(base, relpath)
            assert os.path.exists(target_path)


# These tests are integration-type tests but the behaviour they're
# testing is now easier to test more directly; they should be changed
# to use start_dependent_job_or_raise_if_unfinished
def test_never_started_dependency_exception(workspace, job_spec_maker):
    """Does a never-run dependency mean an exception is raised and the
    dependency is kicked off?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = job_spec_maker(action_id="run_model")
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json={"results": []})
        adapter = m.post("/jobs/", json={})
        with pytest.raises(
            DependencyNotFinished,
            match="Not started because dependency `generate_cohorts` has been added to the job queue",
        ):
            job = Job(job_spec, workdir=project_path)
            job.run_job_and_dependencies()

    assert adapter.request_history[0].json() == {
        "backend": "tpp",
        "force_run": False,
        "force_run_dependencies": False,
        "needed_by_id": 0,
        "action_id": "generate_cohorts",
        "workspace_id": workspace["id"],
    }


def test_unstarted_dependency_exception(job_spec_maker):
    """Does a existing, but unstarted dependency mean an exception is raised?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = job_spec_maker(action_id="run_model")
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


def test_failed_dependency_exception(workspace):
    """Does a existing, but failed dependency mean an exception is raised?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_requested = default_job.copy()
    job_requested.update({"action_id": "run_model", "workspace": workspace})
    existing_failed_job = job_requested.copy()
    existing_failed_job["started"] = True
    existing_failed_job["completed_at"] = "2020-01-01"
    existing_failed_job["status_code"] = 1
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json=test_job_list(job=existing_failed_job))
        with pytest.raises(
            DependencyNotFinished,
            match=r"Dependency `generate_cohorts` failed, so unable to run this action",
        ):
            job = Job(job_requested, workdir=project_path)
            job.run_job_and_dependencies()


@patch("jobrunner.server_interaction.docker_container_exists")
def test_started_dependency_exception(mock_container_exists, job_spec_maker):
    """Does an already-running dependency mean an exception is raised?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = job_spec_maker(action_id="run_model")
    with requests_mock.Mocker() as m:
        m.get("/jobs/", json={"results": []})
        mock_container_exists.return_value = True
        with pytest.raises(
            DependencyNotFinished,
            match=r"Not started because dependency `generate_cohorts` is currently running",
        ):
            job = Job(job_spec, workdir=project_path)
            job.run_job_and_dependencies()


@patch("jobrunner.utils.all_output_paths_for_action")
def test_project_dependency_no_exception(dummy_output_paths, job_spec_maker):
    """Do complete dependencies not raise an exception?

    """
    project_path = "tests/fixtures/simple_project_1"
    job_spec = job_spec_maker(action_id="run_model")
    with tempfile.TemporaryDirectory() as d:
        mock_output_filename = os.path.join(d, "input.csv")
        with open(mock_output_filename, "w") as f:
            f.write("")
        dummy_output_paths.return_value = [("", mock_output_filename)]
        job = Job(job_spec, workdir=project_path)
        job.run_job_and_dependencies()
