import pytest

from jobrunner import queries
from jobrunner.cli import flags
from jobrunner.lib import database


def test_get_no_args(capsys, tmp_work_dir):
    flags.run(["get"])
    stdout, stderr = capsys.readouterr()
    assert stdout == ""
    assert stderr == ""

    queries.set_flag("foo", "bar")
    flags.run(["get"])
    stdout, stderr = capsys.readouterr()
    assert stdout == "foo=bar\n"
    assert stderr == ""


def test_args_get(capsys, tmp_work_dir):
    flags.run(["get", "foo"])
    stdout, stderr = capsys.readouterr()
    assert stdout == "foo=None\n"
    assert stderr == ""

    queries.set_flag("foo", "bar")
    flags.run(["get", "foo"])
    stdout, stderr = capsys.readouterr()
    assert stdout == "foo=bar\n"
    assert stderr == ""


def test_args_set(capsys, tmp_work_dir):
    flags.run(["set", "foo=bar"])
    stdout, stderr = capsys.readouterr()
    assert stdout == "foo=bar\n"
    assert stderr == ""
    assert queries.get_flag("foo") == "bar"


def test_args_set_clear(capsys, tmp_work_dir):
    queries.set_flag("foo", "bar")
    flags.run(["set", "foo="])
    stdout, stderr = capsys.readouterr()
    assert stdout == "foo=None\n"
    assert stderr == ""
    assert queries.get_flag("foo") is None


def test_args_set_create(capsys, tmp_work_dir):
    database.get_connection().execute("DROP TABLE flags")
    with pytest.raises(SystemExit) as e:
        flags.run(["set", "foo=bar"])

    assert "--create" in str(e.value)

    flags.run(["set", "foo=bar", "--create"])
    stdout, stderr = capsys.readouterr()
    assert stdout == "foo=bar\n"
    assert stderr == ""
    assert queries.get_flag("foo") == "bar"
