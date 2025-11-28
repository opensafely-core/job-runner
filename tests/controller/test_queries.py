import time

from controller.lib.database import get_connection
from controller.queries import get_flag_value, set_flag


def test_get_flag_no_table_does_not_error(tmp_work_dir):
    conn = get_connection()
    conn.execute("DROP TABLE IF EXISTS flags")
    assert get_flag_value("foo", backend="foo") is None


def test_get_flag_no_row(tmp_work_dir):
    assert get_flag_value("foo", backend="foo") is None


def test_get_flag_no_row_with_default(tmp_work_dir):
    assert get_flag_value("foo", backend="foo", default="default") == "default"


def test_set_flag(tmp_work_dir):
    assert get_flag_value("foo", backend="foo") is None
    ts1 = set_flag("foo", "bar", backend="foo").timestamp
    assert get_flag_value("foo", backend="foo") == "bar"
    time.sleep(0.01)
    ts2 = set_flag("foo", "bar", backend="foo").timestamp
    # check timestamp has not changed
    assert ts1 == ts2
    set_flag("foo", None, backend="foo")
    assert get_flag_value("foo", backend="foo") is None


def test_set_flag_multiple_backends(tmp_work_dir):
    assert get_flag_value("foo", backend="test1") is None
    set_flag("foo", "bar", backend="test1")
    assert get_flag_value("foo", backend="test1") == "bar"
    assert get_flag_value("foo", backend="test2") is None
    set_flag("foo", "baz", backend="test2")
    assert get_flag_value("foo", backend="test2") == "baz"
