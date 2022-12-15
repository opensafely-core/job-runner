from unittest import mock

from jobrunner.cli import prepare_for_reboot
from jobrunner.executors.local import container_name, volume_api
from jobrunner.lib import database, docker
from jobrunner.models import Job, State, StatusCode
from tests.conftest import get_trace
from tests.factories import job_factory


def test_prepare_for_reboot(db, monkeypatch):

    j1 = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)
    j2 = job_factory(
        state=State.PENDING, status_code=StatusCode.WAITING_ON_DEPENDENCIES
    )

    mocker = mock.MagicMock(spec=docker)
    mockume_api = mock.MagicMock(spec=volume_api)

    monkeypatch.setattr(prepare_for_reboot, "docker", mocker)
    monkeypatch.setattr(prepare_for_reboot, "volume_api", mockume_api)

    prepare_for_reboot.main(pause=False)

    job1 = database.find_one(Job, id=j1.id)
    assert job1.state == State.PENDING
    assert job1.status_code == StatusCode.WAITING_ON_REBOOT
    assert "restarted" in job1.status_message

    job2 = database.find_one(Job, id=j2.id)
    assert job2.state == State.PENDING
    assert job2.status_code == StatusCode.WAITING_ON_DEPENDENCIES

    mocker.kill.assert_called_once_with(container_name(job1))
    mocker.delete_container.assert_called_once_with(container_name(job1))
    mockume_api.delete_volume.assert_called_once_with(job1)

    spans = get_trace("jobs")
    assert spans[-1].name == "EXECUTING"
