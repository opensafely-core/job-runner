import pytest

from jobrunner.database import get_connection, transaction, insert, find_where


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
    with transaction():
        insert("job", job)
    jobs = find_where("job", job_request_id__in=["bar123", "baz123"])
    assert job["id"] == jobs[0]["id"]
    assert job["output_spec_json"] == jobs[0]["output_spec_json"]
