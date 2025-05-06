from unittest import mock

import pytest

from jobrunner.cli import kill_job
from jobrunner.executors import local
from jobrunner.lib import database
from jobrunner.models import Job, State, StatusCode
from tests.factories import job_factory


def test_get_jobs_no_jobs(db):
    # set a string to use as a partial id
    partial_job_id = "1234"
    partial_job_ids = [partial_job_id]

    with pytest.raises(RuntimeError):
        kill_job.get_jobs(partial_job_ids, backend="test")


def test_get_jobs_no_match(db):
    # make a fake job
    job_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, id="z6tkp3mjato63dkm"
    )
    # job that matches, but different backend
    job_factory(
        state=State.RUNNING,
        status_code=StatusCode.EXECUTING,
        id="12345678",
        backend="foo",
    )

    partial_job_id = "1234"
    partial_job_ids = [partial_job_id]

    with pytest.raises(RuntimeError):
        kill_job.get_jobs(partial_job_ids, backend="test")


def test_get_jobs_multiple_matches(db, monkeypatch):
    # make a fake job
    job = job_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, id="z6tkp3mjato63dkm"
    )

    job_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, id="z6tkp3mjato63dkn"
    )

    partial_job_id = "kp3mj"
    partial_job_ids = [partial_job_id]

    monkeypatch.setattr("builtins.input", lambda _: "1")

    output_job_ids = kill_job.get_jobs(partial_job_ids, backend="test")

    assert output_job_ids[0].id == job.id


def test_get_jobs_multiple_params_partial(db, monkeypatch):
    job1 = job_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, id="z6tkp3mjato63dkm"
    )

    job2 = job_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, id="z6tkp3mjato63dkn"
    )

    partial_job_ids = ["dkm", "dkn"]

    monkeypatch.setattr("builtins.input", lambda _: "")

    # search for jobs with our partial id
    output_job_ids = kill_job.get_jobs(partial_job_ids, backend="test")

    assert output_job_ids[0].id == job1.id
    assert output_job_ids[1].id == job2.id


def test_get_jobs_partial_id(db, monkeypatch):
    # make a fake job
    job = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)

    # take the first four characters to make a partial id
    partial_job_id = job.id[:4]
    partial_job_ids = [partial_job_id]

    monkeypatch.setattr("builtins.input", lambda _: "")

    # search for jobs with our partial id
    output_job_ids = kill_job.get_jobs(partial_job_ids, backend="test")

    assert output_job_ids[0].id == job.id


def test_get_jobs_partial_id_quit(db, monkeypatch):
    # make a fake job
    job = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)

    # take the first four characters to make a partial id
    partial_job_id = job.id[:4]
    partial_job_ids = [partial_job_id]

    def press_control_c(_):
        raise KeyboardInterrupt()

    monkeypatch.setattr("builtins.input", press_control_c)

    # make sure the program is quit
    with pytest.raises(KeyboardInterrupt):
        kill_job.get_jobs(partial_job_ids, backend="test")


def test_get_jobs_full_id(db):
    # make a fake job
    job = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)

    # this "partial id" is secretly a full id!!
    full_job_id = job.id
    full_job_ids = [full_job_id]

    # search for jobs with our partial id
    output_job_ids = kill_job.get_jobs(full_job_ids, backend="test")

    assert output_job_ids[0].id == job.id


@pytest.mark.needs_docker
@pytest.mark.parametrize(
    "state,status_code,expected_status_code,cleanup",
    [
        (State.RUNNING, StatusCode.EXECUTING, StatusCode.KILLED_BY_ADMIN, False),
        (State.RUNNING, StatusCode.EXECUTING, StatusCode.KILLED_BY_ADMIN, True),
        (State.PENDING, StatusCode.PREPARING, StatusCode.KILLED_BY_ADMIN, True),
        (State.FAILED, StatusCode.INTERNAL_ERROR, StatusCode.INTERNAL_ERROR, True),
    ],
)
def test_kill_job(
    state, status_code, expected_status_code, cleanup, tmp_work_dir, db, monkeypatch
):
    job = job_factory(state=state, status_code=status_code)

    mocker = mock.MagicMock(spec=local.docker)
    mockumes = mock.MagicMock(spec=local.volumes)

    def mock_get_jobs(partial_job_ids, backend):
        return [job]

    # persist_outputs needs this
    mocker.container_inspect.return_value = {
        "Image": "image",
        "State": {"ExitCode": 137, "OOMKilled": False},
    }

    # set both the docker module names used to the mocker version
    monkeypatch.setattr(kill_job, "docker", mocker)
    monkeypatch.setattr(local, "docker", mocker)
    monkeypatch.setattr(local, "volumes", mockumes)
    monkeypatch.setattr(kill_job, "get_jobs", mock_get_jobs)

    kill_job.main(job.id, cleanup=cleanup)

    job1 = database.find_one(Job, id=job.id)
    assert job1.state == State.FAILED
    assert job1.status_code == expected_status_code

    container = local.container_name(job)
    assert mocker.kill.call_args[0] == (container,)
    assert mocker.write_logs_to_file.call_args[0][0] == container

    log_dir = local.get_log_dir(job)
    log_file = log_dir / "logs.txt"
    metadata_file = log_dir / "metadata.json"

    assert log_file.exists()
    assert metadata_file.exists()

    workspace_log_file = (
        local.get_high_privacy_workspace(job.workspace)
        / local.METADATA_DIR
        / f"{job.action}.log"
    )
    assert not workspace_log_file.exists()

    if cleanup:
        assert mocker.delete_container.called
        assert mockumes.delete_volume.called
    else:
        assert not mocker.delete_container.called
        assert not mockumes.delete_volume.called


@pytest.mark.needs_docker
def test_kill_job_no_container_metadata(tmp_work_dir, db, monkeypatch):
    job = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)

    mocker = mock.MagicMock(spec=local.docker)
    mockumes = mock.MagicMock(spec=local.volumes)

    def mock_get_jobs(partial_job_ids, backend):
        return [job]

    # no container metadata
    mocker.container_inspect.return_value = {}

    # set both the docker module names used to the mocker version
    monkeypatch.setattr(kill_job, "docker", mocker)
    monkeypatch.setattr(local, "docker", mocker)
    monkeypatch.setattr(local, "volumes", mockumes)
    monkeypatch.setattr(kill_job, "get_jobs", mock_get_jobs)

    kill_job.main(job.id, cleanup=True)

    job1 = database.find_one(Job, id=job.id)
    assert job1.state == State.FAILED
    assert job1.status_code == StatusCode.KILLED_BY_ADMIN

    container = local.container_name(job)
    assert mocker.kill.call_args[0] == (container,)

    # No container metadata found, so no logs to write
    mocker.write_logs_to_file.assert_not_called()

    log_dir = local.get_log_dir(job)
    log_file = log_dir / "logs.txt"
    metadata_file = log_dir / "metadata.json"

    assert not log_file.exists()
    assert not metadata_file.exists()

    workspace_log_file = (
        local.get_high_privacy_workspace(job.workspace)
        / local.METADATA_DIR
        / f"{job.action}.log"
    )
    assert not workspace_log_file.exists()

    # The container and volume are still deleted
    assert mocker.delete_container.called
    assert mockumes.delete_volume.called
