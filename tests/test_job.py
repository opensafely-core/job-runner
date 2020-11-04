import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests_mock

from jobrunner import utils
from jobrunner.exceptions import (
    DependencyNotFinished,
    DockerRunError,
    OpenSafelyError,
    RepoNotFound,
)
from jobrunner.job import Job, copy_from_container, volume_from_filespec
from tests.common import default_job, test_job_list


class MyError(OpenSafelyError):
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


def test_exception_reporting():
    error = MyError("thing not to leak", report_args=False)
    assert error.safe_details() == "MyError: [possibly-unsafe details redacted]"
    assert repr(error) == "MyError('thing not to leak')"

    error = MyError("thing OK to leak", report_args=True)
    assert error.safe_details() == "MyError: thing OK to leak"
    assert repr(error) == "MyError('thing OK to leak')"


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


@patch("jobrunner.utils.docker_container_exists")
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


@patch("jobrunner.utils.get_workdir")
@patch("jobrunner.utils.all_output_paths_for_action")
def test_project_dependency_no_exception(
    dummy_output_paths, mock_get_workdir, job_spec_maker
):
    """Do complete dependencies not raise an exception?
    """

    project_path = "tests/fixtures/simple_project_1"
    job_spec = job_spec_maker(action_id="run_model")
    with tempfile.TemporaryDirectory() as d:
        mock_get_workdir.return_value = d
        mock_output_filename = os.path.join(d, "input.csv")
        with open(mock_output_filename, "w") as f:
            f.write("")
        dummy_output_paths.return_value = [
            {"base_path": "", "namespace": "", "relative_path": mock_output_filename}
        ]

        job = Job(job_spec, workdir=project_path)
        job.run_job_and_dependencies()


@patch("jobrunner.utils.get_workdir")
def test_volume_from_filespec_folder(mock_get_workdir):
    with tempfile.TemporaryDirectory() as d:
        mock_get_workdir.return_value = d
        path_1 = Path("a/b/1.txt")
        path_2 = Path("a/b/2.txt")
        path_3 = Path("a/3.txt")
        d = Path(d)
        for f in [path_1, path_2, path_3]:
            f = d / f
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text("1")
        d = Path(d)

        # The `/.` is how `docker cp` expects to be instructed "copy the
        # *contents of*"
        input_file_spec = [(f"{d}/.", ".")]
        with volume_from_filespec(input_file_spec) as volume_info:
            volume_name, container_name = volume_info
            volume_contents = sorted(
                subprocess.check_output(
                    ["docker", "exec", container_name, "find", str(d), "-type", "f"],
                    encoding="utf8",
                ).splitlines()
            )

            assert volume_contents == [
                f"{d}/a/3.txt",
                f"{d}/a/b/1.txt",
                f"{d}/a/b/2.txt",
            ]


@patch("jobrunner.utils.get_workdir")
def test_volume_from_filespec_single_file(mock_get_workdir):
    with tempfile.TemporaryDirectory() as d:
        mock_get_workdir.return_value = d
        path_1 = Path(d) / "a/b/1.txt"
        path_1.parent.mkdir(parents=True, exist_ok=True)
        path_1.write_text("1")

        input_file_spec = [(str(path_1), "path/to/something.txt")]
        with volume_from_filespec(input_file_spec) as volume_info:
            volume_name, container_name = volume_info
            volume_contents = subprocess.check_output(
                [
                    "docker",
                    "exec",
                    container_name,
                    "cat",
                    f"{d}/path/to/something.txt",
                ],
                encoding="utf8",
            )

            assert volume_contents == "1"


def test_copy_from_container():
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        path_1 = d / "a/b/1.txt"
        path_1.parent.mkdir(parents=True, exist_ok=True)
        path_1.write_text("1")

        input_path_tuples = [(str(path_1), "path/to/something.txt")]
        with volume_from_filespec(input_path_tuples) as volume_info:
            volume_name, container_name = volume_info
            copy_from_container(
                container_name,
                [(utils.get_workdir(), f"{d}/new", "path/to/something.*")],
            )
            assert os.path.exists(d / "new/path/to/something.txt")


def test_copy_from_container_raises_when_no_files():
    input_path_tuples = []
    with tempfile.TemporaryDirectory() as d:
        with volume_from_filespec(input_path_tuples) as volume_info:
            volume_name, container_name = volume_info
            d = Path(d)
            with pytest.raises(DockerRunError, match="No expected outputs found"):
                copy_from_container(
                    container_name,
                    [(utils.get_workdir(), f"{d}/new", "path/to/nothing.*")],
                )
