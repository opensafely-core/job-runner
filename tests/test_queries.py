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


def test_get_set_flag(tmp_work_dir):
    assert get_flag_value("foo") is None
    set_flag("foo", "bar")
    assert get_flag_value("foo") == "bar"
    set_flag("foo", None)
    assert get_flag_value("foo") is None
