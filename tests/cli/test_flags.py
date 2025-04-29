import time

import pytest

from jobrunner import queries
from jobrunner.cli import flags
from jobrunner.lib import database
from jobrunner.models import timestamp_to_isoformat


# use a fixed time for these tests
TEST_TIME = time.time()
TEST_DATESTR = timestamp_to_isoformat(TEST_TIME)


@pytest.fixture(autouse=True)
def configure_backends(monkeypatch):
    monkeypatch.setattr("jobrunner.config.common.BACKENDS", ["test_backend"])


def test_get_no_args(capsys, tmp_work_dir):
    flags.run(["--backend", "test_backend", "get"])
    stdout, stderr = capsys.readouterr()
    assert stdout == ""
    assert stderr == ""

    queries.set_flag("foo", "bar", "test_backend", TEST_TIME)
    flags.run(["--backend", "test_backend", "get"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""


def test_args_get(capsys, tmp_work_dir):
    flags.run(["--backend", "test_backend", "get", "foo"])
    stdout, stderr = capsys.readouterr()
    assert stdout == "[test_backend] foo=None (never set)\n"
    assert stderr == ""

    queries.set_flag("foo", "bar", "test_backend", TEST_TIME)
    flags.run(["--backend", "test_backend", "get", "foo"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""


def test_args_set(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    flags.run(["--backend", "test_backend", "set", "foo=bar"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo", "test_backend").value == "bar"


def test_args_set_clear(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    queries.set_flag("foo", "bar", "test_backend")
    flags.run(["--backend", "test_backend", "set", "foo="])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=None ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo", "test_backend").value is None


def test_args_set_error(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    with pytest.raises(SystemExit):
        flags.run(["--backend", "test_backend", "set", "foo"])
    stdout, stderr = capsys.readouterr()
    assert "invalid parse_cli_flag value" in stderr
    assert stdout == ""


def test_args_get_create(capsys, tmp_work_dir):
    database.get_connection().execute("DROP TABLE flags")
    with pytest.raises(SystemExit) as e:
        flags.run(["--backend", "test_backend", "get"])

    assert "--create" in str(e.value)

    flags.run(["--backend", "test_backend", "get", "--create"])
    stdout, stderr = capsys.readouterr()
    assert stdout == ""
    assert stderr == ""


def test_args_set_create(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    database.get_connection().execute("DROP TABLE flags")
    with pytest.raises(SystemExit) as e:
        flags.run(["--backend", "test_backend", "set", "foo=bar"])

    assert "--create" in str(e.value)

    flags.run(["--backend", "test_backend", "set", "foo=bar", "--create"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"[test_backend] foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo", "test_backend").value == "bar"
