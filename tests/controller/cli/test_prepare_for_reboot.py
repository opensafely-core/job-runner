import pytest

from controller.cli import prepare_for_reboot
from controller.models import Job, State, StatusCode, Task, TaskType
from controller.queries import set_flag
from jobrunner.lib import database
from tests.conftest import get_trace
from tests.factories import (
    canceljob_db_task_factory,
    job_factory,
    runjob_db_task_factory,
)


def pause_backend(paused=True):
    set_flag("paused", str(paused), backend="test")


def test_prepare_for_reboot(db):
    pause_backend()
    j1 = job_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, backend="test"
    )
    t1 = runjob_db_task_factory(j1)

    j2 = job_factory(
        state=State.PENDING,
        status_code=StatusCode.WAITING_ON_DEPENDENCIES,
        backend="test",
    )

    # running job with an inactive task
    j3 = job_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, backend="test"
    )
    t2 = runjob_db_task_factory(j3, active=False)

    assert t1.active
    assert not t2.active

    prepare_for_reboot.main("test", require_confirmation=False)

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

    # Only job1 was running with an active runjob task
    cancel_tasks = database.find_where(Task, type=TaskType.CANCELJOB)
    assert len(cancel_tasks) == 1
    assert cancel_tasks[0].id.startswith(job1.id)

    spans = get_trace("jobs")
    assert spans[-1].name == "EXECUTING"


@pytest.mark.parametrize("input_response", ["y", "n"])
def test_prepare_for_reboot_require_confirmation(input_response, db, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: input_response)
    pause_backend()

    j1 = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)
    t1 = runjob_db_task_factory(j1)

    if input_response != "y":
        with pytest.raises(AssertionError):
            prepare_for_reboot.main("test", require_confirmation=True)
    else:
        prepare_for_reboot.main("test", require_confirmation=True)

    job = database.find_one(Job, id=j1.id)
    task = database.find_one(Task, id=t1.id)
    cancel_tasks = database.find_where(Task, type=TaskType.CANCELJOB)

    if input_response == "y":
        assert job.state == State.PENDING
        assert job.status_code == StatusCode.WAITING_ON_REBOOT
        assert not task.active
        assert task.finished_at is not None
        assert len(cancel_tasks) == 1
        assert cancel_tasks[0].id.startswith(j1.id)
    else:
        assert job.state == State.RUNNING
        assert job.status_code == StatusCode.EXECUTING
        assert task.active
        assert task.finished_at is None
        assert len(cancel_tasks) == 0


def test_prepare_for_reboot_backend_not_paused(db):
    t1 = runjob_db_task_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, backend="test"
    )
    j1 = database.find_one(Job, id=t1.id.split("-")[0])

    # Run prepare_for_reboot without pausing the backend; nothing is changed
    prepare_for_reboot.main("test", require_confirmation=False)
    job = database.find_one(Job, id=j1.id)
    task = database.find_one(Task, id=t1.id)
    assert job.state == State.RUNNING
    assert task.active
    assert not database.exists_where(Task, type=TaskType.CANCELJOB)

    # Pause backend and try again
    pause_backend()
    prepare_for_reboot.main("test", require_confirmation=False)
    job = database.find_one(Job, id=j1.id)
    task = database.find_one(Task, id=t1.id)
    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_ON_REBOOT
    assert not task.active
    assert task.finished_at is not None
    assert database.exists_where(Task, type=TaskType.CANCELJOB)


@pytest.mark.parametrize("paused", [True, False])
def test_prepare_for_reboot_status_with_running_job(db, paused, capsys):
    pause_backend(paused)
    # running job
    t1 = runjob_db_task_factory(
        state=State.RUNNING, status_code=StatusCode.EXECUTING, backend="test"
    )
    j1 = database.find_one(Job, id=t1.id.split("-")[0])

    prepare_for_reboot.main("test", status=True, require_confirmation=False)
    job = database.find_one(Job, id=j1.id)
    task = database.find_one(Task, id=t1.id)
    assert job.state == State.RUNNING
    assert task.active
    assert not database.exists_where(Task, type=TaskType.CANCELJOB)

    out, _ = capsys.readouterr()
    assert f"1) backend 'test' is {'not ' if not paused else ''}paused" in out
    assert "2) 1 job(s) are running" in out


@pytest.mark.parametrize("paused", [True, False])
def test_prepare_for_reboot_status_with_cancelled_job(db, paused, capsys):
    pause_backend(paused)
    # reset job
    j1 = job_factory(
        state=State.PENDING, status_code=StatusCode.WAITING_ON_REBOOT, backend="test"
    )
    t1 = canceljob_db_task_factory(job=j1)

    prepare_for_reboot.main("test", status=True, require_confirmation=False)
    job = database.find_one(Job, id=j1.id)
    task = database.find_one(Task, id=t1.id)
    assert job.state == State.PENDING
    assert task.active

    out, _ = capsys.readouterr()
    assert f"1) backend 'test' is {'not ' if not paused else ''}paused" in out
    assert "2) 0 job(s) are running" in out
    assert "3) 1 job(s) are being cancelled" in out


@pytest.mark.parametrize("paused", [True, False])
def test_prepare_for_reboot_status_ready_to_reboot(db, paused, capsys):
    pause_backend(paused)
    # reset job, inactive task
    j1 = job_factory(
        state=State.PENDING, status_code=StatusCode.WAITING_ON_REBOOT, backend="test"
    )
    t1 = canceljob_db_task_factory(job=j1, active=False)

    prepare_for_reboot.main("test", status=True, require_confirmation=False)
    job = database.find_one(Job, id=j1.id)
    task = database.find_one(Task, id=t1.id)
    assert job.state == State.PENDING
    assert not task.active

    out, _ = capsys.readouterr()
    assert f"1) backend 'test' is {'not ' if not paused else ''}paused" in out
    assert "2) 0 job(s) are running" in out
    assert "READY TO REBOOT" in out
    if paused:
        assert "Safe to reboot now"
    else:
        assert "Pause backend 'test' before rebooting"
