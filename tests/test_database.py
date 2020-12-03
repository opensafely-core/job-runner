from jobrunner.database import insert, find_where, update, select_values
from jobrunner.models import Job, State


def test_basic_roundtrip(tmp_work_dir):
    job = Job(
        id="foo123",
        job_request_id="bar123",
        state=State.RUNNING,
        output_spec={"hello": [1, 2, 3]},
    )
    insert(job)
    jobs = find_where(Job, job_request_id__in=["bar123", "baz123"])
    assert job.id == jobs[0].id
    assert job.output_spec == jobs[0].output_spec


def test_update(tmp_work_dir):
    job = Job(id="foo123", action="foo")
    insert(job)
    job.action = "bar"
    update(job, update_fields=["action"])
    jobs = find_where(Job, id="foo123")
    assert jobs[0].action == "bar"


def test_select_values(tmp_work_dir):
    insert(Job(id="foo123", state=State.PENDING))
    insert(Job(id="foo124", state=State.RUNNING))
    insert(Job(id="foo125", state=State.FAILED))
    values = select_values(Job, "id", state__in=[State.PENDING, State.FAILED])
    assert sorted(values) == ["foo123", "foo125"]
    values = select_values(Job, "state", id="foo124")
    assert values == [State.RUNNING]
