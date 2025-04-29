from unittest import mock

import pytest

from jobrunner.cli import retry_job
from jobrunner.models import State, StatusCode
from tests.factories import job_factory


def test_get_jobs_no_jobs(db):
    # set a string to use as a partial id
    partial_job_id = "1234"
    with pytest.raises(RuntimeError):
        retry_job.get_job(partial_job_id, backend="test")


def test_get_job_no_match(db):
    # make a fake job
    job_factory(
        state=State.FAILED, status_code=StatusCode.INTERNAL_ERROR, id="z6tkp3mjato63dkm"
    )
    partial_job_id = "1234"
    with pytest.raises(RuntimeError):
        retry_job.get_job(partial_job_id, backend="test")


def test_get_job_multiple_matches(db, monkeypatch):
    # make a fake job
    job = job_factory(
        state=State.FAILED, status_code=StatusCode.INTERNAL_ERROR, id="z6tkp3mjato63dkm"
    )

    job_factory(
        state=State.FAILED, status_code=StatusCode.INTERNAL_ERROR, id="z6tkp3mjato63dkn"
    )

    partial_job_id = "kp3mj"

    monkeypatch.setattr("builtins.input", lambda _: "1")

    output_job = retry_job.get_job(partial_job_id, backend="test")

    assert output_job.id == job.id


@pytest.mark.needs_docker
def test_main_no_running_container(db, monkeypatch):
    job_factory(
        state=State.FAILED, status_code=StatusCode.INTERNAL_ERROR, id="g6tkp3mjato63dkm"
    )
    monkeypatch.setattr("builtins.input", lambda _: "")
    with pytest.raises(RuntimeError, match="associated container does not exist"):
        retry_job.main("g6tk")


@mock.patch("jobrunner.cli.retry_job.docker.container_exists", return_value=True)
@mock.patch("jobrunner.cli.retry_job.api_post")
def test_main(mock_api_post, mock_container_exists, db, monkeypatch):
    job = job_factory(
        state=State.FAILED, status_code=StatusCode.INTERNAL_ERROR, id="l6tkp3mjato63dkm"
    )
    partial_job_id = "l6tk"
    monkeypatch.setattr("builtins.input", lambda _: "")
    retry_job.main(partial_job_id)
    job = retry_job.get_job(partial_job_id, backend="test")
    assert job.status_code == StatusCode.EXECUTING
    assert job.state == State.RUNNING
    mock_api_post.assert_called_once()
