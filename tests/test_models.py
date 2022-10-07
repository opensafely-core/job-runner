from datetime import datetime

from tests.factories import job_factory


def test_job_asdict_timestamps(db):
    actual_time = "2022-10-07T14:59:12.345678+0000"
    fmt = "%Y-%m-%dT%H:%M:%S.%f%z"
    dt = datetime.strptime(actual_time, fmt)
    ts = dt.timestamp()

    job = job_factory(created_at=int(ts), status_code_updated_at=int(ts * 1e9))
    d = job.asdict()

    assert d["created_at"] == "2022-10-07T14:59:12Z"
    assert d["status_code_updated_at"] == "2022-10-07T14:59:12.345678Z"
