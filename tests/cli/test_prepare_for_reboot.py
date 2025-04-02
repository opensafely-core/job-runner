from unittest import mock

import pytest

from jobrunner.cli import prepare_for_reboot
from jobrunner.executors import local, volumes
from jobrunner.lib import database, docker
from jobrunner.models import Job, State, StatusCode
from tests.conftest import get_trace
from tests.factories import job_factory


@pytest.mark.needs_docker
def test_prepare_for_reboot(db, monkeypatch):
    j1 = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)
    j2 = job_factory(
        state=State.PENDING, status_code=StatusCode.WAITING_ON_DEPENDENCIES
    )

    mocker = mock.MagicMock(spec=docker)
    mockumes = mock.MagicMock(spec=volumes)

    monkeypatch.setattr(prepare_for_reboot, "docker", mocker)
    monkeypatch.setattr(prepare_for_reboot, "volumes", mockumes)

    prepare_for_reboot.main(pause=False)

    job1 = database.find_one(Job, id=j1.id)
    assert job1.state == State.PENDING
    assert job1.status_code == StatusCode.WAITING_ON_REBOOT
    assert "restarted" in job1.status_message

    job2 = database.find_one(Job, id=j2.id)
    assert job2.state == State.PENDING
    assert job2.status_code == StatusCode.WAITING_ON_DEPENDENCIES

    mocker.kill.assert_called_once_with(local.container_name(job1))
    mocker.delete_container.assert_called_once_with(local.container_name(job1))
    mockumes.delete_volume.assert_called_once_with(job1)

    spans = get_trace("jobs")
    assert spans[-1].name == "EXECUTING"


@pytest.mark.needs_docker
@pytest.mark.parametrize("input_response", ["y", "n"])
def test_prepare_for_reboot_pause(input_response, db, monkeypatch):
    j1 = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)

    mocker = mock.MagicMock(spec=docker)
    mockumes = mock.MagicMock(spec=volumes)

    monkeypatch.setattr(prepare_for_reboot, "docker", mocker)
    monkeypatch.setattr(prepare_for_reboot, "volumes", mockumes)
    monkeypatch.setattr("builtins.input", lambda _: input_response)

    if input_response != "y":
        with pytest.raises(AssertionError):
            prepare_for_reboot.main(pause=True)
    else:
        prepare_for_reboot.main(pause=True)

    job = database.find_one(Job, id=j1.id)
    if input_response == "y":
        assert job.state == State.PENDING
        assert job.status_code == StatusCode.WAITING_ON_REBOOT
    else:
        assert job.state == State.RUNNING
        assert job.status_code == StatusCode.EXECUTING
