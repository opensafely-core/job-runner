import pytest

from jobrunner.database import get_connection, insert, find_where, update


@pytest.fixture(autouse=True)
def temp_db(monkeypatch, tmp_path):
    monkeypatch.setattr("jobrunner.config.DATABASE_FILE", tmp_path / "db.sqlite")
    get_connection.cache_clear()


def test_basic_roundtrip():
    job = {
        "id": "foo123",
        "job_request_id": "bar123",
        "output_spec_json": {"hello": [1, 2, 3]},
    }
    insert("job", job)
    jobs = find_where("job", job_request_id__in=["bar123", "baz123"])
    assert job["id"] == jobs[0]["id"]
    assert job["output_spec_json"] == jobs[0]["output_spec_json"]


def test_update():
    insert("job", {"id": "foo123", "action": "foo"})
    insert("job", {"id": "foo124", "action": "bar"})
    update("job", {"action": "baz"}, id="foo123")
    jobs = find_where("job", id="foo123")
    assert jobs[0]["action"] == "baz"
