from datetime import datetime

import pytest

from controller.models import StatusCode
from tests.factories import (
    job_factory,
    job_request_factory_raw,
)


def test_job_asdict_timestamps(db):
    actual_time = "2022-10-07T14:59:12.345678+0000"
    fmt = "%Y-%m-%dT%H:%M:%S.%f%z"
    dt = datetime.strptime(actual_time, fmt)
    ts = dt.timestamp()

    job = job_factory(created_at=int(ts), status_code_updated_at=int(ts * 1e9))
    d = job.asdict()

    assert d["created_at"] == "2022-10-07T14:59:12Z"
    assert d["status_code_updated_at"] == "2022-10-07T14:59:12.345678Z"


@pytest.mark.parametrize(
    "value,default,expected",
    [
        ("preparing", None, StatusCode.PREPARING),
        ("prepared", None, StatusCode.PREPARED),
        ("executing", None, StatusCode.EXECUTING),
        ("unknown", None, None),
        ("unknown", StatusCode.CREATED, StatusCode.CREATED),
    ],
)
def test_status_code_from_value(value, default, expected):
    kwargs = {}
    if default is not None:
        kwargs = {"default": default}
    assert StatusCode.from_value(value, **kwargs) == expected


def test_job_request_get_tracing_span_attributes():
    job_request = job_request_factory_raw()
    span_attributes = job_request.get_tracing_span_attributes()
    assert span_attributes == {
        "backend": "test",
        "workspace": "workspace",
        "user": "testuser",
        "project": "project",
        "orgs": ["org1", "org2"],
    }
