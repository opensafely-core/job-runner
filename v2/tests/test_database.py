import pytest

from jobrunner.database import get_connection, insert, find_where, update
from jobrunner.models import Job, State


@pytest.fixture(autouse=True)
def temp_db(monkeypatch, tmp_path):
    monkeypatch.setattr("jobrunner.config.DATABASE_FILE", tmp_path / "db.sqlite")
    get_connection.cache_clear()


def test_basic_roundtrip():
    job = Job(
        id="foo123",
        job_request_id="bar123",
        status=State.RUNNING,
        output_spec={"hello": [1, 2, 3]},
    )
    insert(job)
    jobs = find_where(Job, job_request_id__in=["bar123", "baz123"])
    assert job.id == jobs[0].id
    assert job.output_spec == jobs[0].output_spec


def test_update():
    job = Job(id="foo123", action="foo")
    insert(job)
    job.action = "bar"
    update(job, update_fields=["action"])
    jobs = find_where(Job, id="foo123")
    assert jobs[0].action == "bar"
