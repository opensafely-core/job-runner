import pytest

from jobrunner.lib.database import find_one, insert, select_values, update
from jobrunner.models import Job, State


def test_basic_roundtrip(tmp_work_dir):
    job = Job(
        id="foo123",
        job_request_id="bar123",
        state=State.RUNNING,
        output_spec={"hello": [1, 2, 3]},
    )
    insert(job)
    j = find_one(Job, job_request_id__in=["bar123", "baz123"])
    assert job.id == j.id
    assert job.output_spec == j.output_spec


def test_update(tmp_work_dir):
    job = Job(id="foo123", action="foo")
    insert(job)
    job.action = "bar"
    update(job)
    assert find_one(Job, id="foo123").action == "bar"


def test_update_excluding_a_field(tmp_work_dir):
    job = Job(id="foo123", action="foo", commit="commit-of-glory")
    insert(job)
    job.action = "bar"
    job.commit = "commit-of-doom"
    update(job, exclude_fields=["commit"])
    j = find_one(Job, id="foo123")
    assert j.action == "bar"
    assert j.commit == "commit-of-glory"


def test_select_values(tmp_work_dir):
    insert(Job(id="foo123", state=State.PENDING))
    insert(Job(id="foo124", state=State.RUNNING))
    insert(Job(id="foo125", state=State.FAILED))
    values = select_values(Job, "id", state__in=[State.PENDING, State.FAILED])
    assert sorted(values) == ["foo123", "foo125"]
    values = select_values(Job, "state", id="foo124")
    assert values == [State.RUNNING]


def test_find_one_returns_a_single_value(tmp_work_dir):
    insert(Job(id="foo123", workspace="the-workspace"))
    job = find_one(Job, id="foo123")
    assert job.workspace == "the-workspace"


def test_find_one_fails_if_there_are_no_results(tmp_work_dir):
    with pytest.raises(ValueError):
        find_one(Job, id="foo123")


def test_find_one_fails_if_there_is_more_than_one_result(tmp_work_dir):
    insert(Job(id="foo123", workspace="the-workspace"))
    insert(Job(id="foo456", workspace="the-workspace"))
    with pytest.raises(ValueError):
        find_one(Job, workspace="the-workspace")
