import os
import time
from unittest.mock import patch

import opentelemetry.exporter.otlp.proto.http.trace_exporter
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import ConsoleSpanExporter

from jobrunner import models, tracing
from jobrunner.config import common as common_config
from tests.conftest import get_trace
from tests.factories import job_factory, job_request_factory, job_task_results_factory


def test_setup_default_tracing_empty_env(monkeypatch):
    env = {}
    monkeypatch.setattr(os, "environ", env)
    provider = tracing.setup_default_tracing(set_global=False)
    assert provider._active_span_processor._span_processors == ()


def test_setup_default_tracing_console(monkeypatch):
    env = {"OTEL_EXPORTER_CONSOLE": "1"}
    monkeypatch.setattr(os, "environ", env)
    provider = tracing.setup_default_tracing(set_global=False)

    processor = provider._active_span_processor._span_processors[0]
    assert isinstance(processor.span_exporter, ConsoleSpanExporter)


def test_setup_default_tracing_otlp_defaults(monkeypatch):
    env = {"OTEL_EXPORTER_OTLP_HEADERS": "'foo=bar'"}
    monkeypatch.setattr(os, "environ", env)
    monkeypatch.setattr(
        opentelemetry.exporter.otlp.proto.http.trace_exporter, "environ", env
    )
    provider = tracing.setup_default_tracing(set_global=False)
    assert provider.resource.attributes["service.name"] == "jobrunner"

    exporter = provider._active_span_processor._span_processors[0].span_exporter
    assert isinstance(exporter, OTLPSpanExporter)
    assert exporter._endpoint == "https://api.honeycomb.io/v1/traces"
    assert exporter._headers == {"foo": "bar"}
    assert env["OTEL_EXPORTER_OTLP_ENDPOINT"] == "https://api.honeycomb.io"


def test_setup_default_tracing_otlp_with_env(monkeypatch):
    env = {
        "OTEL_EXPORTER_OTLP_HEADERS": "foo=bar",
        "OTEL_SERVICE_NAME": "service",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "https://endpoint",
    }
    monkeypatch.setattr(os, "environ", env)
    monkeypatch.setattr(
        opentelemetry.exporter.otlp.proto.http.trace_exporter, "environ", env
    )
    provider = tracing.setup_default_tracing(set_global=False)
    assert provider.resource.attributes["service.name"] == "service"

    exporter = provider._active_span_processor._span_processors[0].span_exporter

    assert isinstance(exporter, OTLPSpanExporter)
    assert exporter._endpoint == "https://endpoint/v1/traces"
    assert exporter._headers == {"foo": "bar"}


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

    assert attrs == dict(
        backend="test",
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
        created_at=int(job.created_at * 1e9),
        started_at=None,
        status_code_updated_at=job.status_code_updated_at,
        reusable_action="action_repo:commit",
        requires_db=False,
        exit_code=1,
        image_id="image_id",
        executor_message="message",
        action_version="unknown",
        action_revision="unknown",
        action_created="unknown",
        base_revision="unknown",
        base_created="unknown",
    )


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

    assert attrs == dict(
        backend="test",
        job=job.id,
        job_request=job.job_request_id,
        workspace="workspace",
        action="action",
        commit="abc123def",
        run_command=job.run_command,
        user="testuser",
        project="project",
        orgs="org1,org2",
        state="PENDING",
        message="message",
        created_at=int(job.created_at * 1e9),
        started_at=None,
        status_code_updated_at=job.status_code_updated_at,
        requires_db=False,
    )


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


def test_initialise_trace(db):
    job = job_factory()
    # clear factories default context
    job.trace_context = None

    tracing.initialise_trace(job)

    assert "traceparent" in job.trace_context

    # check we can load it without error
    tracing.load_root_span(job)

    # check is has not been emitted
    spans = get_trace("jobs")
    assert len(spans) == 0


def test_initialise_trace_does_not_use_current_span(db):
    job = job_factory()
    # clear factories default context
    job.trace_context = None

    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("test_initialise_trace") as current_span:
        tracing.initialise_trace(job)

    # check that we did not use the current spans trace id
    span_context = tracing.load_root_span(job)
    assert span_context.trace_id != current_span.context.trace_id


def test_finish_current_state(db):
    job = job_factory()
    start_time = job.status_code_updated_at
    results = job_task_results_factory()

    ts = int(time.time() * 1e9)

    tracing.finish_current_state(job, ts, results=results, extra="extra")

    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"
    assert spans[-1].start_time == start_time
    assert spans[-1].end_time == ts
    assert spans[-1].attributes["extra"] == "extra"
    assert spans[-1].attributes["job"] == job.id
    assert spans[-1].attributes["is_state"] is True
    assert spans[-1].attributes["exit_code"] == 0


def test_record_final_state(db):
    job = job_factory(status_code=models.StatusCode.SUCCEEDED)
    results = job_task_results_factory()
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
    results = job_task_results_factory(exit_code=1)
    tracing.record_final_state(job, ts, error=Exception("error"), results=results)

    spans = get_trace("jobs")
    assert spans[-2].name == "INTERNAL_ERROR"
    assert spans[-2].status.status_code.name == "ERROR"
    assert spans[-2].events[0].name == "exception"
    assert spans[-2].events[0].attributes["exception.message"] == "error"
    assert not spans[-2].status.is_ok
    assert spans[-2].attributes["exit_code"] == 1

    assert spans[-1].name == "JOB"
    assert spans[-1].status.status_code.name == "ERROR"
    assert spans[-1].events[0].name == "exception"
    assert spans[-1].events[0].attributes["exception.message"] == "error"
    assert not spans[-1].status.is_ok
    assert spans[-1].attributes["exit_code"] == 1


def test_record_job_span_skips_uninitialized_job(db):
    job = job_factory()
    ts = int(time.time() * 1e9)
    job.trace_context = None

    tracing.record_job_span(job, "name", ts, ts + 10000, error=None, results=None)

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
        assert span.attributes["exit_code"] == 1


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

    assert not span.status.is_ok
    assert span.status.description == "test"
    assert span.events[0].name == "exception"
    assert span.events[0].attributes["exception.message"] == "test"


def test_set_span_metadata_non_recording_span_with_invalid_attribute_type(db, caplog):
    # This is a test for a previous bug, where logging an invalid type for a
    # a non-recording span attempted to call span.name (non-recording spans have no
    # name attribute)
    job = job_factory()
    non_recording_span = trace.NonRecordingSpan({})
    tracing.set_span_metadata(non_recording_span, job, bar=dict())
    assert "attribute bar was set invalid type: {}" in caplog.text


def test_set_span_metadata_invalid_attribute_type(db, caplog):
    job = job_factory()
    tracer = trace.get_tracer("test")
    span = tracer.start_span("test")
    tracing.set_span_metadata(span, job, foo=None, bar=dict(), foobar=set())
    assert "attribute foo was set invalid type" not in caplog.text
    assert "attribute bar was set invalid type: {}" in caplog.text
    assert "attribute foobar was set invalid type: set()" in caplog.text
    assert span.attributes["foo"] == "None"
    assert span.attributes["bar"] == "{}"
    assert span.attributes["foobar"] == "set()"


def test_set_span_metadata_tracing_errors_do_not_raise(db, caplog):
    job = job_factory()
    tracer = trace.get_tracer("test")

    span = tracer.start_span("test")
    # mock Exception raised in function called by set_span_metadata
    with patch("jobrunner.tracing.trace_attributes", side_effect=Exception("foo")):
        tracing.set_span_metadata(span, job, error=Exception("test"))

    assert f"failed to trace job {job.id}" in caplog.text


def test_record_final_state_tracing_errors_do_not_raise(db, caplog):
    job = job_factory()
    ts = int(time.time() * 1e9)
    results = job_task_results_factory()
    # mock Exception raised in function called by set_span_metadata
    with patch("jobrunner.tracing.complete_job", side_effect=Exception("foo")):
        tracing.record_final_state(job, ts, error=Exception("error"), results=results)

    assert f"failed to trace state for {job.id}" in caplog.text


def test_finish_current_state_tracing_errors_do_not_raise(db, caplog):
    job = job_factory()
    ts = int(time.time() * 1e9)
    results = job_task_results_factory()
    # mock Exception raised in function called by set_span_metadata
    with patch("jobrunner.tracing.record_job_span", side_effect=Exception("foo")):
        tracing.finish_current_state(job, ts, results=results)

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
