import time

from jobrunner import models, tracing
from tests.conftest import get_trace
from tests.factories import job_factory, job_request_factory


def test_trace_attributes(db):

    jr = job_request_factory(
        original=dict(
            created_by="testuser",
            project="project",
            orgs=["org1", "org2"],
        )
    )
    job = job_factory(
        jr,
        workspace="workspace",
        action="action",
        commit="commit",
        action_repo_url="action_repo",
        action_commit="commit",
        status_message="message",
    )

    attrs = tracing.trace_attributes(job)

    assert attrs == dict(
        backend="expectations",
        job=job.id,
        job_request=job.job_request_id,
        workspace="workspace",
        action="action",
        commit="commit",
        run_command=job.run_command,
        user="testuser",
        project="project",
        orgs="org1,org2",
        state="PENDING",
        message="message",
        reusable_action="action_repo:commit",
    )


def test_initialise_trace(db):
    job = job_factory()
    # clear factories default context
    job.trace_context = None

    tracing.initialise_trace(job)

    assert "traceparent" in job.trace_context

    spans = get_trace()
    assert spans[-1].name == "ENTER CREATED"


def test_finish_current_state(db):
    job = job_factory()

    ts = int(time.time() * 1e9)

    tracing.finish_current_state(job, ts, extra="extra")

    spans = get_trace()
    assert spans[-1].name == "CREATED"
    assert spans[-1].start_time == int(job.created_at * 1e9)
    assert spans[-1].end_time == ts
    assert spans[-1].attributes["extra"] == "extra"
    assert spans[-1].attributes["job"] == job.id


def test_record_final_state(db):
    job = job_factory(status_code=models.StatusCode.SUCCEEDED)
    ts = int(time.time() * 1e9)
    tracing.record_final_state(job, ts)

    spans = get_trace()
    assert spans[-2].name == "SUCCEEDED"
    assert spans[-1].name == "RUN"


def test_record_final_state_error(db):
    job = job_factory(status_code=models.StatusCode.INTERNAL_ERROR)
    ts = int(time.time() * 1e9)
    tracing.record_final_state(job, ts, error=Exception("error"))

    spans = get_trace()
    assert spans[-2].name == "INTERNAL_ERROR"
    assert spans[-2].status.status_code.name == "ERROR"
    assert spans[-2].events[0].name == "exception"
    assert spans[-2].events[0].attributes["exception.message"] == "error"

    assert spans[-1].name == "RUN"
    assert spans[-1].status.status_code.name == "ERROR"
    assert spans[-1].events[0].name == "exception"
    assert spans[-1].events[0].attributes["exception.message"] == "error"


def test_start_new_state(db):
    job = job_factory()
    ts = int(time.time() * 1e9)

    job.status_code = models.StatusCode.PREPARING

    tracing.start_new_state(job, ts)

    spans = get_trace()
    assert spans[-1].name == "ENTER PREPARING"
    assert spans[-1].attributes["enter_state"] is True
    assert spans[-1].end_time == int(ts + 1e9)
