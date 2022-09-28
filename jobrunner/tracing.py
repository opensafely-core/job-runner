import logging
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from jobrunner import config
from jobrunner.lib import database
from jobrunner.models import Job, SavedJobRequest, StatusCode


logger = logging.getLogger(__name__)
provider = TracerProvider()
trace.set_tracer_provider(provider)


def add_exporter(exporter, processor=SimpleSpanProcessor):
    # we default to SimpleSpanProcessor so that it's synchronous for tests
    provider.add_span_processor(processor(exporter))


def setup_default_tracing():
    """Inspect environmenet variables and set up exporters accordingly.

    We use the SimpleSpanProcessor by default, which is synchronous. The
    BatchSpanProcess is asynchronous and more efficent at scale, but are
    emitting so few spans that it is easier just to keep it simpler, and it
    makes testing easier.
    """
    if "OTEL_EXPORTER_OTLP_HEADERS" in os.environ:
        if "OTEL_SERVICE_NAME" not in os.environ:
            raise Exception(
                "OTEL_EXPORTER_OTLP_HEADERS is configured, but missing OTEL_SERVICE_NAME"
            )
        if "OTEL_EXPORTER_OTLP_ENDPOINT" not in os.environ:
            os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://api.honeycomb.io"

        add_exporter(OTLPSpanExporter())

    if "OTEL_EXPORTER_CONSOLE" in os.environ:
        add_exporter(ConsoleSpanExporter())


def trace_attributes(job):
    """These attributes are added to every span in order to slice and dice by
    each as needed.
    """
    # grab job request metadata, caching it on the job instance to avoid excess
    # queries/jsoning
    if job._job_request is None:
        try:
            job._job_request = database.find_one(
                SavedJobRequest, id=job.job_request_id
            ).original
        except ValueError:
            job._job_request = {}

    attrs = dict(
        backend=config.BACKEND,
        job=job.id,
        job_request=job.job_request_id,
        workspace=job.workspace,
        action=job.action,
        commit=job.commit,
        run_command=job.run_command,
        user=job._job_request.get("created_by", "unknown"),
        project=job._job_request.get("project", "unknown"),
        orgs=",".join(job._job_request.get("orgs", [])),
        state=job.state.name,
        message=job.status_message,
    )

    if job.action_repo_url:
        attrs["reusable_action"] = job.action_repo_url
        if job.action_commit:
            attrs["reusable_action"] += ":" + job.action_commit

    # opentelemetry cannot handle None values
    return {k: "" if v is None else v for k, v in attrs.items()}


def initialise_trace(job):
    """Initialise the trace for this job by creating a root span.

    We store the serialised trace context in the db, so we can reuse it for
    later spans.

    We create a root span, which is a requirement in OTel. For this reason we
    send it out straight away, which means its duration is very short.
    """
    assert not job.trace_context, "this job already has a trace-context"
    assert job.status_code is not None, "job has no initial StatusCode"
    assert (
        job.status_code_updated_at is not None
    ), "job has no initial status_code_updated_at"

    job.trace_context = {}
    tracer = trace.get_tracer("jobs")
    attrs = trace_attributes(job)

    # convert created_at to nanoseconds
    start_time = int(job.created_at * 1e9)

    # TraceContextTextMapPropagator only works with the current span, sadly
    with tracer.start_as_current_span("job", start_time=start_time) as root:
        root.set_attributes(attrs)
        # we serialise the entire trace context, as it may grow extra fields
        # (e.g.  baggage) over time
        TraceContextTextMapPropagator().inject(job.trace_context)

    # trace the initial job state trace
    start_new_state(job, job.status_code_updated_at)


def finish_current_state(job, timestamp_ns, error=None, **attrs):
    """Record a span representing the state we've just exited."""
    try:
        name = job.status_code.name
        start_time = job.status_code_updated_at
        record_job_span(job, name, start_time, timestamp_ns, error, **attrs)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace state for {job.id}")


def record_final_state(job, timestamp_ns, error=None, **attrs):
    """Record a span representing the state we've just exited."""
    try:
        name = job.status_code.name
        # Note: this *must* be timestamp as integer nanoseconds
        start_time = job.status_code_updated_at

        # final states have no duration, so make last for 1 sec, just act
        # as a marker
        end_time = int(timestamp_ns + 1e9)
        record_job_span(job, name, start_time, end_time, error, **attrs)

        # record a full span for the entire run
        # trace vanity: have the job start 1us before the actual job, so
        # it shows up first in the trace
        job_start_time = int(job.created_at * 1e9) - 1000
        record_job_span(job, "RUN", job_start_time, timestamp_ns, error, **attrs)

    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace state for {job.id}")


def start_new_state(job, timestamp_ns, error=None, **attrs):
    """Record a marker span to say that we've entered a new state."""
    try:
        name = f"ENTER {job.status_code.name}"
        start_time = timestamp_ns
        # fix the time for these synthetic marker events at one second
        end_time = int(start_time + 1e9)
        if attrs is None:
            attrs = {}
        # allow them to be filtered out easily
        attrs["enter_state"] = True
        record_job_span(job, name, start_time, end_time, error, **attrs)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace state for {job.id}")


def record_job_span(job, name, start_time, end_time, error, **attrs):
    """Record a span for a job."""
    assert job.trace_context is not None, "job missing trace_context"
    ctx = TraceContextTextMapPropagator().extract(carrier=job.trace_context)
    tracer = trace.get_tracer("jobs")

    attributes = {}

    if attrs:
        attributes.update(attrs)
    attributes.update(trace_attributes(job))

    span = tracer.start_span(
        name,
        context=ctx,
        start_time=start_time,
    )
    span.set_attributes(attributes)
    if error:
        span.set_status(trace.Status(trace.StatusCode.ERROR))
    if isinstance(error, Exception):
        span.record_exception(error)

    span.end(end_time)


if __name__ == "__main__":
    # local testing utility for tracing
    import time

    from jobrunner.run import set_code

    setup_default_tracing()

    timestamp = int(time.time())
    job = Job(
        id="job_id",
        status_code=StatusCode.CREATED,
        status_code_updated_at=int(timestamp * 1e9),
        job_request_id="request_id",
        workspace="workspace",
        action="action name",
        run_command="cohortextractor:latest cmd opt",
        commit="commit",
        created_at=timestamp,
    )
    initialise_trace(job)

    states = [
        StatusCode.WAITING_ON_DEPENDENCIES,
        StatusCode.PREPARING,
        StatusCode.PREPARED,
        StatusCode.EXECUTING,
        StatusCode.EXECUTED,
        StatusCode.FINALIZING,
        StatusCode.FINALIZED,
    ]

    for state in states:
        time.sleep(1.1)
        set_code(job, state, "test")

    time.sleep(1.1)
    set_code(job, StatusCode.SUCCEEDED, "success")
