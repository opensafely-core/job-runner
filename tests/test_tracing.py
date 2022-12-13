import time

from opentelemetry import trace

from jobrunner import models, tracing
from tests.conftest import get_trace
from tests.factories import job_factory, job_request_factory, job_results_factory


def test_trace_attributes(db, monkeypatch):
    monkeypatch.setattr(tracing.config, "VERSION", "v1.2.3")
    monkeypatch.setattr(tracing.config, "GIT_SHA", "abcdefg")

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

    results = job_results_factory(
        outputs=["foo", "bar"],
        unmatched_patterns=["unmatched_patterns"],
        unmatched_outputs=["unmatched_outputs"],
        exit_code=1,
        image_id="image_id",
        message="message",
    )

    attrs = tracing.trace_attributes(job, results)

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
        requires_db=False,
        outputs=2,
        unmatched_patterns=1,
        unmatched_outputs=1,
        exit_code=1,
        image_id="image_id",
        executor_message="message",
        jobrunner_version="v1.2.3",
        jobrunner_sha="abcdefg",
        action_version="unknown",
        action_revision="unknown",
        action_created="unknown",
        base_revision="unknown",
        base_created="unknown",
    )


def test_trace_attributes_missing(db, monkeypatch):
    monkeypatch.setattr(tracing.config, "VERSION", "v1.2.3")
    monkeypatch.setattr(tracing.config, "GIT_SHA", "abcdefg")

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
        status_message="message",
        # no commit
        # no reusable action
    )

    attrs = tracing.trace_attributes(job)

    assert attrs == dict(
        backend="expectations",
        job=job.id,
        job_request=job.job_request_id,
        workspace="workspace",
        action="action",
        run_command=job.run_command,
        user="testuser",
        project="project",
        orgs="org1,org2",
        state="PENDING",
        message="message",
        requires_db=False,
        jobrunner_version="v1.2.3",
        jobrunner_sha="abcdefg",
    )


def test_initialise_trace(db):
    job = job_factory()
    # clear factories default context
    job.trace_context = None

    tracing.initialise_trace(job)

    assert "traceparent" in job.trace_context

    spans = get_trace("jobs")
    assert len(spans) == 0


def test_finish_current_state(db):
    job = job_factory()
    results = job_results_factory()

    ts = int(time.time() * 1e9)

    tracing.finish_current_state(job, ts, results=results, extra="extra")

    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"
    assert spans[-1].start_time == int(job.created_at * 1e9)
    assert spans[-1].end_time == ts
    assert spans[-1].attributes["extra"] == "extra"
    assert spans[-1].attributes["job"] == job.id
    assert spans[-1].attributes["is_state"] is True
    assert spans[-1].attributes["exit_code"] == 0


def test_record_final_state(db):
    job = job_factory(status_code=models.StatusCode.SUCCEEDED)
    results = job_results_factory()
    ts = int(time.time() * 1e9)
    tracing.record_final_state(job, ts, results=results)

    spans = get_trace("jobs")
    assert spans[-2].name == "SUCCEEDED"
    assert spans[-2].attributes["exit_code"] == 0
    assert spans[-1].name == "JOB"
    assert spans[-1].attributes["exit_code"] == 0


def test_record_final_state_error(db):
    job = job_factory(status_code=models.StatusCode.INTERNAL_ERROR)
    ts = int(time.time() * 1e9)
    results = job_results_factory(exit_code=1)
    tracing.record_final_state(job, ts, error=Exception("error"), results=results)

    spans = get_trace("jobs")
    assert spans[-2].name == "INTERNAL_ERROR"
    assert spans[-2].status.status_code.name == "ERROR"
    assert spans[-2].events[0].name == "exception"
    assert spans[-2].events[0].attributes["exception.message"] == "error"
    assert spans[-2].status.status_code == trace.StatusCode.ERROR
    assert spans[-2].attributes["exit_code"] == 1

    assert spans[-1].name == "JOB"
    assert spans[-1].status.status_code.name == "ERROR"
    assert spans[-1].events[0].name == "exception"
    assert spans[-1].events[0].attributes["exception.message"] == "error"
    assert spans[-1].status.status_code == trace.StatusCode.ERROR
    assert spans[-1].attributes["exit_code"] == 1


def test_start_new_state(db):
    job = job_factory()
    ts = int(time.time() * 1e9)

    job.status_code = models.StatusCode.PREPARING

    tracing.start_new_state(job, ts)

    spans = get_trace("jobs")
    assert spans[-1].name == "ENTER PREPARING"
    assert spans[-1].attributes["is_state"] is False
    assert spans[-1].end_time == int(ts + 1e9)

    # deprecated attribute
    assert spans[-1].attributes["enter_state"] is True


def test_record_job_span_skips_uninitialized_job(db):
    job = job_factory()
    ts = int(time.time() * 1e9)
    job.trace_context = None

    tracing.record_job_span(job, "name", ts, ts + 10000, error=None, results=None)

    assert len(get_trace("jobs")) == 0


def test_complete_job(db):
    job = job_factory()
    results = job_results_factory(exit_code=1)
    ts = int(time.time() * 1e9)

    tracing.complete_job(job, ts, results=results)

    ctx = tracing.load_root_span(job)

    spans = get_trace("jobs")
    assert spans[0].name == "JOB"
    assert spans[0].context.trace_id == ctx.trace_id
    assert spans[0].context.span_id == ctx.span_id
    assert spans[0].parent is None
    assert spans[0].attributes["exit_code"] == 1


def test_set_span_metadata_attrs(db):
    job_request = job_request_factory()
    job = job_factory(job_request=job_request)
    tracer = trace.get_tracer("test")

    class Test:
        def __str__(self):
            return "test"

    span = tracer.start_span("test")
    tracing.set_span_metadata(
        span,
        job,
        custom_attr=Test(),  # test that attr is added and the type coerced to string
        state="should be ignored",  # test that we can't override core job attributes
    )

    assert span.attributes["job"] == job.id
    assert span.attributes["job_request"] == job.job_request_id
    assert span.attributes["workspace"] == job.workspace
    assert span.attributes["action"] == job.action
    assert span.attributes["state"] == job.state.name  # not "should be ignored"
    assert span.attributes["custom_attr"] == "test"

    # job request attrs
    assert span.attributes["user"] == job_request.original["created_by"]
    assert span.attributes["project"] == job_request.original["project"]
    assert span.attributes["orgs"] == ",".join(job_request.original["orgs"])


def test_set_span_metadata_error(db):
    job = job_factory()
    tracer = trace.get_tracer("test")

    span = tracer.start_span("test")
    tracing.set_span_metadata(span, job, error=Exception("test"))

    assert span.status.status_code == trace.StatusCode.ERROR
    assert span.status.description == "test"
    assert span.events[0].name == "exception"
    assert span.events[0].attributes["exception.message"] == "test"
