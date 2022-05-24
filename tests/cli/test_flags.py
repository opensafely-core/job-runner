import time

import pytest

from jobrunner import queries
from jobrunner.cli import flags
from jobrunner.lib import database
from jobrunner.models import timestamp_to_isoformat


# use a fixed time for these tests
TEST_TIME = time.time()
TEST_DATESTR = timestamp_to_isoformat(TEST_TIME)


def test_get_no_args(capsys, tmp_work_dir):
    flags.run(["get"])
    stdout, stderr = capsys.readouterr()
    assert stdout == ""
    assert stderr == ""

    queries.set_flag("foo", "bar", TEST_TIME)
    flags.run(["get"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""


def test_args_get(capsys, tmp_work_dir):
    flags.run(["get", "foo"])
    stdout, stderr = capsys.readouterr()
    assert stdout == "foo=None (never set)\n"
    assert stderr == ""

    queries.set_flag("foo", "bar", TEST_TIME)
    flags.run(["get", "foo"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""


def test_args_set(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    flags.run(["set", "foo=bar"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo").value == "bar"


def test_args_set_clear(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    queries.set_flag("foo", "bar")
    flags.run(["set", "foo="])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"foo=None ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo").value is None


def test_args_get_create(capsys, tmp_work_dir):
    database.get_connection().execute("DROP TABLE flags")
    with pytest.raises(SystemExit) as e:
        flags.run(["get"])

    assert "--create" in str(e.value)

    flags.run(["get", "--create"])
    stdout, stderr = capsys.readouterr()
    assert stdout == ""
    assert stderr == ""


def test_args_set_create(capsys, tmp_work_dir, freezer):
    freezer.move_to(TEST_DATESTR)
    database.get_connection().execute("DROP TABLE flags")
    with pytest.raises(SystemExit) as e:
        flags.run(["set", "foo=bar"])

    assert "--create" in str(e.value)

    flags.run(["set", "foo=bar", "--create"])
    stdout, stderr = capsys.readouterr()
    assert stdout == f"foo=bar ({TEST_DATESTR})\n"
    assert stderr == ""
    assert queries.get_flag("foo").value == "bar"
