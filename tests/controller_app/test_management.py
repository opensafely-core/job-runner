"""
These management commands are wrappers around jobrunner/cli/controller and
core functionality is tested in tests/cli/controller; these tests just ensure
that they run correctly via management command.
"""

from django.core.management import call_command

from jobrunner import queries
from jobrunner.lib import database
from jobrunner.models import Job
from tests.cli.controller.test_flags import TEST_DATESTR, TEST_TIME


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
    call_command("flags", "set", "foo=bar", "--backend", "test")
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo", "test").value == "bar"
