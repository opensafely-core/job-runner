import time

import pytest

from controller import queries
from controller.models import timestamp_to_isoformat
from jobrunner.cli.controller import flags
from jobrunner.lib import database


# use a fixed time for these tests
TEST_TIME = time.time()
TEST_DATESTR = timestamp_to_isoformat(TEST_TIME)


@pytest.fixture(autouse=True)
def configure_backends(monkeypatch):
    monkeypatch.setattr("common.config.BACKENDS", ["test_backend"])


def test_get_no_args(capsys, tmp_work_dir):
    flags.run(["get", "--backend", "test_backend"])
    stdout, stderr = capsys.readouterr()
    assert stdout == ""
    assert stderr == ""

    queries.set_flag("foo", "bar", "test_backend", TEST_TIME)
    flags.run(["get", "--backend", "test_backend"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""


def test_args_get(capsys, tmp_work_dir):
    flags.run(["get", "foo", "--backend", "test_backend"])
    stdout, stderr = capsys.readouterr()
    assert stdout == "[test_backend] foo=None (never set)\n"
    assert stderr == ""

    queries.set_flag("foo", "bar", "test_backend", TEST_TIME)
    flags.run(["get", "--backend", "test_backend", "foo"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""


def test_args_set(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    flags.run(["set", "foo=bar", "--backend", "test_backend"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo", "test_backend").value == "bar"


def test_args_set_clear(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    queries.set_flag("foo", "bar", "test_backend")
    flags.run(["set", "--backend", "test_backend", "foo="])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=None ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo", "test_backend").value is None


def test_args_set_error(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    with pytest.raises(SystemExit):
        flags.run(["set", "foo", "--backend", "test_backend"])
    stdout, stderr = capsys.readouterr()
    assert "invalid parse_cli_flag value" in stderr
    assert stdout == ""


def test_args_get_create(capsys, tmp_work_dir):
    database.get_connection().execute("DROP TABLE flags")
    with pytest.raises(SystemExit) as e:
        flags.run(["get", "--backend", "test_backend"])

    assert "--create" in str(e.value)

    flags.run(["get", "--create", "--backend", "test_backend"])
    stdout, stderr = capsys.readouterr()
    assert stdout == ""
    assert stderr == ""


def test_args_set_create(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    database.get_connection().execute("DROP TABLE flags")
    with pytest.raises(SystemExit) as e:
        flags.run(["set", "foo=bar", "--backend", "test_backend"])

    assert "--create" in str(e.value)

    flags.run(["set", "foo=bar", "--create", "--backend", "test_backend"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo", "test_backend").value == "bar"
