import logging
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import propagation
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from jobrunner import config
from jobrunner.lib import database
from jobrunner.models import Job, SavedJobRequest, State, StatusCode


logger = logging.getLogger(__name__)
provider = TracerProvider()
trace.set_tracer_provider(provider)


def add_exporter(exporter, processor=BatchSpanProcessor):
    """Utility method to add an exporter.

    We use the BatchSpanProcessor by default, which is the default for
    production. This is asynchronous, and queues and retries sending telemetry.

    In testing, we insteads use SimpleSpanProcessor, which is synchronous and
    easy to inspect the output of within a test.
    """
    # Note: BatchSpanProcessor is configured via env vars:
    # https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.export.html#opentelemetry.sdk.trace.export.BatchSpanProcessor
    provider.add_span_processor(processor(exporter))


def setup_default_tracing():
    """Inspect environment variables and set up exporters accordingly."""
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

    # create a root span in order to have a parent for all subsequent spans.
    # However, we do not annotate or emit this span object now. We do this when
    # the job has completed; see complete_job() for details.
    root = tracer.start_span("JOB")

    # TraceContextTextMapPropagator only works with the current span, so set it as such.
    with trace.use_span(root, end_on_exit=False):
        # we serialise the entire trace context, as it may grow extra fields
        # (e.g.  baggage) over time
        TraceContextTextMapPropagator().inject(job.trace_context)


def _traceable(job):
    """Is a job traceable?

    Helper function to handle switching to tracing code when there are jobs
    running that pre-existed it.
    """
    if job.trace_context is None or job.status_code is None:
        logger.info(f"not tracing job {job.id} as not initialised")
        return False

    return True


def finish_current_state(job, timestamp_ns, error=None, **attrs):
    """Record a span representing the state we've just exited."""
    if not _traceable(job):
        return

    # allow them to be filtered out from tracking spans
    attrs["is_state"] = True
    try:
        name = job.status_code.name
        start_time = job.status_code_updated_at
        record_job_span(job, name, start_time, timestamp_ns, error, **attrs)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace state for {job.id}")


def record_final_state(job, timestamp_ns, error=None, **attrs):
    """Record a span representing the state we've just exited."""
    if not _traceable(job):
        return

    try:
        name = job.status_code.name
        # Note: this *must* be timestamp as integer nanoseconds
        start_time = job.status_code_updated_at

        # final states have no duration, so make last for 1 sec, just act
        # as a marker
        end_time = int(timestamp_ns + 1e9)
        record_job_span(job, name, start_time, end_time, error, **attrs)

        complete_job(job, timestamp_ns, error, **attrs)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace state for {job.id}")


def start_new_state(job, timestamp_ns, error=None, **attrs):
    """Record a marker span to say that we've entered a new state."""
    if not _traceable(job):
        return

    # allow them to be filtered out easily
    attrs["is_state"] = False
    # legacy filter, remove once above is deployed for ~1 week
    attrs["enter_state"] = True

    try:
        name = f"ENTER {job.status_code.name}"
        start_time = timestamp_ns
        # fix the time for these synthetic marker events at one second
        end_time = int(start_time + 1e9)
        record_job_span(job, name, start_time, end_time, error, **attrs)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace state for {job.id}")


def load_trace_context(job):
    """Load the trace for this job from the db.

    Returns a context object, which is suitable for feeding into a span's
    context argument.
    """
    # The OTel propagation is designed to propagate across process/service
    # boundaries. As such, using the extract() function returns a span context
    # with is_remote=True.  However, we are using propagation to serialize
    # trace context within a process, so we do not want this.
    #
    # However, there is no easy way to change it, as SpanContext is immutable.
    # So we recreate an identical SpanContext, but with is_remote=False
    ctx = TraceContextTextMapPropagator().extract(carrier=job.trace_context)

    orig_ctx = propagation.get_current_span(ctx).get_span_context()
    span_context = trace.SpanContext(
        trace_id=orig_ctx.trace_id,
        span_id=orig_ctx.span_id,
        is_remote=False,
        trace_flags=orig_ctx.trace_flags,
        trace_state=orig_ctx.trace_state,
    )
    return propagation.set_span_in_context(trace.NonRecordingSpan(span_context), {})


def record_job_span(job, name, start_time, end_time, error, **attrs):
    """Record a span for a job."""
    if not _traceable(job):
        return

    ctx = load_trace_context(job)
    tracer = trace.get_tracer("jobs")
    span = tracer.start_span(name, context=ctx, start_time=start_time)
    set_span_metadata(span, job, error, **attrs)
    span.end(end_time)


def complete_job(job, timestamp_ns, error, **attrs):
    """Send the root span to record the full duration for this job."""

    ctx = load_trace_context(job)
    root_ctx = propagation.get_current_span(ctx).get_span_context()
    tracer = trace.get_tracer("jobs")

    # trace vanity: have the job start 1us before the actual job, so
    # it shows up first in the trace
    job_start_time = int(job.created_at * 1e9) - 1000

    # We created this root span at the start of the trace, as is required for
    # a trace, and every span has had it as its id as its parent span. However,
    # there is no easy way to serialize the actual span object now that we want
    # to send it.
    #
    # So we create a new span, and then explicitly set its context to be the
    # original root span's context.
    root_span = tracer.start_span("JOB", start_time=job_start_time, context=ctx)
    root_span._context = root_ctx
    set_span_metadata(root_span, job, error, **attrs)
    root_span.end(timestamp_ns)


def set_span_metadata(span, job, error, **attrs):
    """Set span metadata with everthing we know about a job."""
    attributes = {}

    if attrs:
        attributes.update(attrs)
    attributes.update(trace_attributes(job))

    span.set_attributes(attributes)
    if error:
        span.set_status(trace.Status(trace.StatusCode.ERROR))
    if isinstance(error, Exception):
        span.record_exception(error)


if __name__ == "__main__":
    # local testing utility for tracing
    import time

    from jobrunner.run import set_code

    setup_default_tracing()

    timestamp = int(time.time())
    job = Job(
        id="job_id",
        state=State.PENDING,
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
