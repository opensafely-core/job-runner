"""
These management commands are wrappers around jobrunner/cli/controller and
core functionality is tested in tests/cli/controller; these tests just ensure
that they run correctly via management command.
"""

from django.core.management import call_command

from controller import queries
from controller.models import Job, State, StatusCode, Task, TaskType
from jobrunner.lib import database
from tests.cli.controller.test_flags import TEST_DATESTR, TEST_TIME
from tests.cli.controller.test_prepare_for_reboot import pause_backend
from tests.factories import job_factory, runjob_db_task_factory


def test_add_job(monkeypatch, tmp_work_dir, db, test_repo):
    assert not database.exists_where(Job)
    call_command("add_job", str(test_repo.path), "generate_dataset", backend="test")

    db_jobs = database.find_all(Job)
    assert len(db_jobs) == 1
    assert db_jobs[0].action == "generate_dataset"


def test_flags_get(capsys, db):
    call_command("flags", "get", "foo", backend="test")
    stdout, stderr = capsys.readouterr()
    assert stdout == "[test] foo=None (never set)\n"
    assert stderr == ""

    queries.set_flag("foo", "bar", "test", TEST_TIME)
    call_command("flags", "get", "foo", backend="test")
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""


def test_flags_set(capsys, db, freezer):
    freezer.move_to(TEST_DATESTR)
    call_command("flags", "set", "foo=bar", backend="test")
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo", "test").value == "bar"


def test_prepare_for_reboot(db, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "y")
    pause_backend()

    j1 = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)
    t1 = runjob_db_task_factory(j1)

    call_command("prepare_for_reboot", backend="test")

    job = database.find_one(Job, id=j1.id)
    task = database.find_one(Task, id=t1.id)
    cancel_tasks = database.find_where(Task, type=TaskType.CANCELJOB)

    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_ON_REBOOT
    assert not task.active
    assert task.finished_at is not None
    assert len(cancel_tasks) == 1
    assert cancel_tasks[0].id.startswith(j1.id)


def test_migrate(tmp_path, caplog):
    caplog.set_level("INFO")
    temp_db_file = tmp_path / "test.db"
    call_command("migrate_controller", dbpath=temp_db_file)
    assert f"created new db at {temp_db_file}" in caplog.text, caplog.text


def test_pause(db, freezer, capsys, monkeypatch):
    monkeypatch.setattr("common.config.BACKENDS", ["test", "test1"])
    freezer.move_to(TEST_DATESTR)
    # pause test backend
    call_command("pause", "on", "test")
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test] paused=true ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("paused", "test").value == "true"

    # unpause test backend
    call_command("pause", "off", "test")
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test] paused=None ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("paused", "test").value is None

    # pause a different backend
    call_command("pause", "on", "test1")
    assert queries.get_flag("paused", "test").value is None
    assert queries.get_flag("paused", "test1").value == "true"


def test_db_maintenance(db, freezer, capsys):
    freezer.move_to(TEST_DATESTR)
    call_command("db_maintenance", "on", "test")
    stdout, stderr = capsys.readouterr()
    assert (
        stdout
        == f"[test] mode=db-maintenance ({TEST_DATESTR})\n[test] manual-db-maintenance=on ({TEST_DATESTR})\n"
    )
    assert stderr == ""
    assert queries.get_flag("mode", "test").value == "db-maintenance"
    assert queries.get_flag("manual-db-maintenance", "test").value == "on"

    call_command("db_maintenance", "off", "test")
    stdout, stderr = capsys.readouterr()
    assert (
        stdout
        == f"[test] mode=None ({TEST_DATESTR})\n[test] manual-db-maintenance=None ({TEST_DATESTR})\n"
    )
    assert stderr == ""
    assert queries.get_flag("mode", "test").value is None
    assert queries.get_flag("manual-db-maintenance", "test").value is None
