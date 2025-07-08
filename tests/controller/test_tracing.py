import os
import time
from unittest.mock import patch

from opentelemetry import trace

from common import config as common_config
from controller import models, tracing
from tests.conftest import get_trace
from tests.factories import job_factory, job_request_factory, job_task_results_factory


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
        backend="test",
    )

    results = job_task_results_factory(
        exit_code=1,
        image_id="image_id",
        message="message",
    )

    attrs = tracing.trace_attributes(job, results)

    assert attrs == {
        "job.backend": "test",
        "job.id": job.id,
        "job.request": job.job_request_id,
        "job.workspace": "workspace",
        "job.action": "action",
        "job.commit": "commit",
        "job.run_command": job.run_command,
        "job.user": "testuser",
        "job.project": "project",
        "job.orgs": "org1,org2",
        "job.state": "PENDING",
        "job.message": "message",
        "job.created_at": int(job.created_at * 1e9),
        "job.started_at": None,
        "job.status_code_updated_at": job.status_code_updated_at,
        "job.reusable_action": "action_repo:commit",
        "job.requires_db": False,
        "job.exit_code": 1,
        "job.image_id": "image_id",
        "job.executor_message": "message",
        "job.action_version": "unknown",
        "job.action_revision": "unknown",
        "job.action_created": "unknown",
        "job.base_revision": "unknown",
        "job.base_created": "unknown",
    }


def test_trace_attributes_missing(db):
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
        commit="abc123def",
        backend="test",
        # no reusable action
    )

    attrs = tracing.trace_attributes(job)

    assert attrs == {
        "job.backend": "test",
        "job.id": job.id,
        "job.request": job.job_request_id,
        "job.workspace": "workspace",
        "job.action": "action",
        "job.commit": "abc123def",
        "job.run_command": job.run_command,
        "job.user": "testuser",
        "job.project": "project",
        "job.orgs": "org1,org2",
        "job.state": "PENDING",
        "job.message": "message",
        "job.created_at": int(job.created_at * 1e9),
        "job.started_at": None,
        "job.status_code_updated_at": job.status_code_updated_at,
        "job.requires_db": False,
    }


def test_tracing_resource_config():
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("test") as span:
        pass

    span = get_trace("test")[0]
    assert span.resource.attributes["service.name"] == "jobrunner"
    assert span.resource.attributes["service.namespace"] == os.environ.get(
        "BACKEND", "unknown"
    )
    assert span.resource.attributes["service.version"] == common_config.VERSION


def test_initialise_job_trace(db):
    job = job_factory()
    # clear factories default context
    job.trace_context = None

    tracing.initialise_job_trace(job)

    assert "traceparent" in job.trace_context

    # check we can load it without error
    tracing.load_root_span(job)

    # check is has not been emitted
    spans = get_trace("jobs")
    assert len(spans) == 0


def test_initialise_job_trace_does_not_use_current_span(db):
    job = job_factory()
    # clear factories default context
    job.trace_context = None

    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("test_initialise_job_trace") as current_span:
        tracing.initialise_job_trace(job)

    # check that we did not use the current spans trace id
    span_context = tracing.load_root_span(job)
    assert span_context.trace_id != current_span.context.trace_id


def test_finish_current_job_state(db):
    job = job_factory()
    start_time = job.status_code_updated_at
    results = job_task_results_factory()

    ts = int(time.time() * 1e9)

    tracing.finish_current_job_state(
        job, ts, results=results, extra={"job.extra": "extra"}
    )

    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"
    assert spans[-1].start_time == start_time
    assert spans[-1].end_time == ts
    assert spans[-1].attributes["job.extra"] == "extra"
    assert spans[-1].attributes["job.id"] == job.id
    assert spans[-1].attributes["job.exit_code"] == 0


def test_record_final_job_state_success(db):
    job = job_factory(status_code=models.StatusCode.SUCCEEDED)
    results = job_task_results_factory()
    ts = int(time.time() * 1e9)
    tracing.record_final_job_state(job, ts, results=results)

    spans = get_trace("jobs")
    assert spans[-2].name == "SUCCEEDED"
    assert spans[-2].attributes["job.exit_code"] == 0
    assert spans[-2].attributes["job.succeeded"] is True
    assert spans[-2].status.is_ok

    assert spans[-1].name == "JOB"
    assert spans[-1].attributes["job.exit_code"] == 0
    assert spans[-1].attributes["job.succeeded"] is True
    assert spans[-2].status.is_ok


def test_record_final_job_state_job_failure(db):
    job = job_factory(status_code=models.StatusCode.NONZERO_EXIT)
    ts = int(time.time() * 1e9)
    results = job_task_results_factory(exit_code=1)
    tracing.record_final_job_state(job, ts, results=results)

    spans = get_trace("jobs")
    assert spans[-2].name == "NONZERO_EXIT"
    assert spans[-2].attributes["job.exit_code"] == 1
    assert spans[-2].attributes["job.succeeded"] is False
    assert spans[-2].status.is_ok

    assert spans[-1].name == "JOB"
    assert spans[-1].attributes["job.exit_code"] == 1
    assert spans[-1].attributes["job.succeeded"] is False
    assert spans[-2].status.is_ok


def test_record_final_job_state_internal_error(db):
    job = job_factory(status_code=models.StatusCode.INTERNAL_ERROR)
    ts = int(time.time() * 1e9)
    results = job_task_results_factory(exit_code=1)
    tracing.record_final_job_state(
        job, ts, exception=Exception("error"), results=results
    )

    spans = get_trace("jobs")
    assert spans[-2].name == "INTERNAL_ERROR"
    assert spans[-2].events[0].name == "exception"
    assert spans[-2].events[0].attributes["exception.message"] == "error"
    assert not spans[-2].status.is_ok
    assert spans[-2].attributes["job.exit_code"] == 1
    assert spans[-2].attributes["job.succeeded"] is False

    assert spans[-1].name == "JOB"
    assert spans[-1].events[0].name == "exception"
    assert spans[-1].events[0].attributes["exception.message"] == "error"
    assert not spans[-1].status.is_ok
    assert spans[-1].attributes["job.exit_code"] == 1
    assert spans[-1].attributes["job.succeeded"] is False


def test_record_job_span_skips_uninitialized_job(db):
    job = job_factory()
    ts = int(time.time() * 1e9)
    job.trace_context = None

    tracing.record_job_span(job, "name", ts, ts + 10000, exception=None, results=None)

    assert len(get_trace("jobs")) == 0


def test_complete_job(db):
    job = job_factory()
    results = job_task_results_factory(exit_code=1)
    ts = int(time.time() * 1e9)

    # send span with no current span
    tracing.complete_job(job, ts, results=results)

    # send span with current active span, to ensure it doesn't pick it up as parent span
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("other"):
        tracing.complete_job(job, ts, results=results)

    ctx = tracing.load_root_span(job)

    for span in get_trace("jobs"):
        assert span.name == "JOB"
        assert span.context.trace_id == ctx.trace_id
        assert span.context.span_id == ctx.span_id
        assert span.parent is None
        assert span.attributes["job.exit_code"] == 1


def test_set_span_job_metadata_attrs(db):
    job_request = job_request_factory()
    job = job_factory(job_request=job_request)
    tracer = trace.get_tracer("test")

    class Test:
        def __str__(self):
            return "test"

    span = tracer.start_span("test")
    tracing.set_span_job_metadata(
        span,
        job,
        extra={
            "job.custom_attr": Test(),  # test that attr is added and the type coerced to string
            "job.state": "should be ignored",  # test that we can't override core job attributes
        },
    )

    assert span.attributes["job.id"] == job.id
    assert span.attributes["job.request"] == job.job_request_id
    assert span.attributes["job.workspace"] == job.workspace
    assert span.attributes["job.action"] == job.action
    assert span.attributes["job.state"] == job.state.name  # not "should be ignored"
    assert span.attributes["job.custom_attr"] == "test"

    # job request attrs
    assert span.attributes["job.user"] == job_request.original["created_by"]
    assert span.attributes["job.project"] == job_request.original["project"]
    assert span.attributes["job.orgs"] == ",".join(job_request.original["orgs"])


def test_set_span_job_metadata_attrs_bwcompat(db):
    job_request = job_request_factory()
    job = job_factory(job_request=job_request)
    tracer = trace.get_tracer("test")

    span = tracer.start_span("test")
    tracing.set_span_job_metadata(span, job, extra={"job.custom_attr": "test"})

    # special cased
    assert span.attributes["job"] == job.id
    assert span.attributes["job_request"] == job.job_request_id

    assert span.attributes["workspace"] == job.workspace
    assert span.attributes["action"] == job.action
    assert span.attributes["state"] == job.state.name
    assert span.attributes["custom_attr"] == "test"

    # job request attrs
    assert span.attributes["user"] == job_request.original["created_by"]
    assert span.attributes["project"] == job_request.original["project"]
    assert span.attributes["orgs"] == ",".join(job_request.original["orgs"])


def test_set_span_job_metadata_failure(db):
    job = job_factory()
    tracer = trace.get_tracer("test")

    span = tracer.start_span("test")
    tracing.set_span_job_metadata(span, job, exception=Exception("test"))

    assert span.status.is_ok
    assert span.events[0].name == "exception"
    assert span.events[0].attributes["exception.message"] == "test"


def test_set_span_job_metadata_internal_error(db):
    job = job_factory(status_code=models.StatusCode.INTERNAL_ERROR)
    tracer = trace.get_tracer("test")

    span = tracer.start_span("test")
    tracing.set_span_job_metadata(span, job, exception=Exception("test"))

    assert not span.status.is_ok
    assert span.status.description == "test"
    assert span.events[0].name == "exception"
    assert span.events[0].attributes["exception.message"] == "test"


def test_set_span_job_metadata_non_recording_span_with_invalid_attribute_type(
    db, caplog
):
    # This is a test for a previous bug, where logging an invalid type for a
    # a non-recording span attempted to call span.name (non-recording spans have no
    # name attribute)
    job = job_factory()
    non_recording_span = trace.NonRecordingSpan({})
    tracing.set_span_job_metadata(non_recording_span, job, extra={"job.bar": dict()})
    assert "attribute job.bar was set invalid type: {}" in caplog.text


def test_set_span_job_metadata_invalid_attribute_type(db, caplog):
    job = job_factory()
    tracer = trace.get_tracer("test")
    span = tracer.start_span("test")
    tracing.set_span_job_metadata(
        span, job, extra={"job.foo": None, "job.bar": dict(), "job.foobar": set()}
    )
    assert "attribute job.foo was set invalid type" not in caplog.text
    assert "attribute job.bar was set invalid type: {}" in caplog.text
    assert "attribute job.foobar was set invalid type: set()" in caplog.text
    assert span.attributes["job.foo"] == "None"
    assert span.attributes["job.bar"] == "{}"
    assert span.attributes["job.foobar"] == "set()"


def test_set_span_job_metadata_tracing_errors_do_not_raise(db, caplog):
    job = job_factory()
    tracer = trace.get_tracer("test")

    span = tracer.start_span("test")
    # mock Exception raised in function called by set_span_job_metadata
    with patch("controller.tracing.trace_attributes", side_effect=Exception("foo")):
        tracing.set_span_job_metadata(span, job)

    assert f"failed to trace job {job.id}" in caplog.text


def test_record_final_job_state_tracing_errors_do_not_raise(db, caplog):
    job = job_factory()
    ts = int(time.time() * 1e9)
    results = job_task_results_factory()
    # mock Exception raised in function called by set_span_job_metadata
    with patch("controller.tracing.complete_job", side_effect=Exception("foo")):
        tracing.record_final_job_state(job, ts, results=results)

    assert f"failed to trace state for {job.id}" in caplog.text


def test_finish_current_job_state_tracing_errors_do_not_raise(db, caplog):
    job = job_factory()
    ts = int(time.time() * 1e9)
    results = job_task_results_factory()
    # mock Exception raised in function called by set_span_job_metadata
    with patch("controller.tracing.record_job_span", side_effect=Exception("foo")):
        tracing.finish_current_job_state(job, ts, results=results)

    assert f"failed to trace state for {job.id}" in caplog.text


def test_traceable(db):
    job = job_factory()
    job.status_code = None
    job.trace_context = None
    assert tracing._traceable(job) is False
    job.trace_context = {}
    assert tracing._traceable(job) is False
    job.trace_context = {"foo": "bar"}
    assert tracing._traceable(job) is False
    job.status_code = models.StatusCode.EXECUTING
    assert tracing._traceable(job)
