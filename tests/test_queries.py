import time

from jobrunner.lib.database import get_connection
from jobrunner.queries import get_flag_value, set_flag


def test_get_flag_no_table_does_not_error(tmp_work_dir):
    conn = get_connection()
    conn.execute("DROP TABLE IF EXISTS flags")
    assert get_flag_value("foo") is None


def test_get_flag_no_row(tmp_work_dir):
    assert get_flag_value("foo") is None


def test_get_flag_no_row_with_default(tmp_work_dir):
    assert get_flag_value("foo", "default") == "default"


def test_set_flag(tmp_work_dir):
    assert get_flag_value("foo") is None
    ts1 = set_flag("foo", "bar").timestamp
    assert get_flag_value("foo") == "bar"
    time.sleep(0.01)
    ts2 = set_flag("foo", "bar").timestamp
    # check timestamp has not changed
    assert ts1 == ts2
    set_flag("foo", None)
    assert get_flag_value("foo") is None
