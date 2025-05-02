from unittest import mock

import pytest

from jobrunner.cli import prepare_for_reboot
from jobrunner.executors import local, volumes
from jobrunner.lib import database, docker
from jobrunner.models import Job, State, StatusCode, Task
from tests.conftest import get_trace
from tests.factories import job_factory, runjob_db_task_factory


@pytest.mark.needs_docker
def test_prepare_for_reboot(db, monkeypatch):
    t1 = runjob_db_task_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, backend="test"
    )
    j1 = database.find_one(Job, id=t1.id.split("-")[0])
    j2 = job_factory(
        state=State.PENDING,
        status_code=StatusCode.WAITING_ON_DEPENDENCIES,
        backend="test",
    )
    # running job with an inactive task
    t2 = runjob_db_task_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, backend="test"
    )
    t2.active = False
    database.update(t2)
    assert t2.finished_at is None
    j3 = database.find_one(Job, id=t2.id.split("-")[0])

    assert t1.active
    assert not t2.active

    mocker = mock.MagicMock(spec=docker)
    mockumes = mock.MagicMock(spec=volumes)

    monkeypatch.setattr(prepare_for_reboot, "docker", mocker)
    monkeypatch.setattr(prepare_for_reboot, "volumes", mockumes)

    prepare_for_reboot.main(pause=False)

    job1 = database.find_one(Job, id=j1.id)
    assert job1.state == State.PENDING
    assert job1.status_code == StatusCode.WAITING_ON_REBOOT
    assert "restarted" in job1.status_message

    task1 = database.find_one(Task, id=t1.id)
    assert not task1.active
    assert task1.finished_at is not None

    job2 = database.find_one(Job, id=j2.id)
    assert job2.state == State.PENDING
    assert job2.status_code == StatusCode.WAITING_ON_DEPENDENCIES

    task2 = database.find_one(Task, id=t2.id)
    assert not task2.active
    # task2 was inactive, so prepare for reboot has not updated finished_at either
    assert task2.finished_at is None

    job3 = database.find_one(Job, id=j3.id)
    assert job1.state == State.PENDING
    assert job1.status_code == StatusCode.WAITING_ON_REBOOT
    assert "restarted" in job3.status_message

    mocker.kill.call_args_list == [
        local.container_name(job1),
        local.container_name(job3),
    ]
    mocker.delete_container.call_args_list == [
        local.container_name(job1),
        local.container_name(job3),
    ]
    mocker.delete_volume.call_args_list == [job1, job3]

    spans = get_trace("jobs")
    assert spans[-1].name == "EXECUTING"


@pytest.mark.needs_docker
@pytest.mark.parametrize("input_response", ["y", "n"])
def test_prepare_for_reboot_pause(input_response, db, monkeypatch):
    t1 = runjob_db_task_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)
    j1 = database.find_one(Job, id=t1.id.split("-")[0])

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
    task = database.find_one(Task, id=t1.id)
    if input_response == "y":
        assert job.state == State.PENDING
        assert job.status_code == StatusCode.WAITING_ON_REBOOT
        assert not task.active
        assert task.finished_at is not None
    else:
        assert job.state == State.RUNNING
        assert job.status_code == StatusCode.EXECUTING
        assert task.active
        assert task.finished_at is None
