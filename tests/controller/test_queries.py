import time

from controller.queries import get_flag_value, get_saved_job_request, set_flag
from jobrunner.lib.database import get_connection
from tests.factories import job_factory, job_request_factory, job_request_factory_raw


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


def test_get_saved_job_request(db):
    job_request = job_request_factory()
    job = job_factory()
    assert get_saved_job_request(job) == job_request.original


def test_get_saved_job_request_no_match(db):
    # create a job with an un-saved job_request
    job_request = job_request_factory_raw()
    job = job_factory(job_request=job_request)
    assert get_saved_job_request(job) == {}
