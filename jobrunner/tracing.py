import logging
import os
import warnings
from datetime import datetime

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import propagation
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from jobrunner.config import common as common_config
from jobrunner.lib import warn_assertions
from jobrunner.models import Job, State, StatusCode
from jobrunner.queries import get_saved_job_request


logger = logging.getLogger(__name__)


def get_provider():
    # https://github.com/open-telemetry/semantic-conventions/tree/main/docs/resource#service
    resource = Resource.create(
        attributes={
            "service.name": os.environ.get("OTEL_SERVICE_NAME", "jobrunner"),
            # Note this will be set in the agent only
            "service.namespace": os.environ.get("BACKEND", "unknown"),
            "service.version": common_config.VERSION,
        }
    )
    return TracerProvider(resource=resource)


def add_exporter(provider, exporter, processor=BatchSpanProcessor):
    """Utility method to add an exporter.

    We use the BatchSpanProcessor by default, which is the default for
    production. This is asynchronous, and queues and retries sending telemetry.

    In testing, we insteads use SimpleSpanProcessor, which is synchronous and
    easy to inspect the output of within a test.
    """
    # Note: BatchSpanProcessor is configured via env vars:
    # https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.export.html#opentelemetry.sdk.trace.export.BatchSpanProcessor
    provider.add_span_processor(processor(exporter))


def setup_default_tracing(set_global=True):
    """Inspect environment variables and set up exporters accordingly."""

    provider = get_provider()

    if "OTEL_EXPORTER_OTLP_HEADERS" in os.environ:
        # workaround for env file parsing issues
        cleaned_headers = os.environ["OTEL_EXPORTER_OTLP_HEADERS"].strip("\"'")
        # put back into env to be parsed properly
        os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = cleaned_headers

        if "OTEL_EXPORTER_OTLP_ENDPOINT" not in os.environ:
            os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://api.honeycomb.io"

        # now we can created OTLP exporter
        add_exporter(provider, OTLPSpanExporter())

    if "OTEL_EXPORTER_CONSOLE" in os.environ:
        add_exporter(provider, ConsoleSpanExporter())

    if set_global:
        trace.set_tracer_provider(provider)  # pragma: no cover

    # bug: this code requires some envvars to be set, so ensure they are
    os.environ.setdefault("PYTHONPATH", "")
    from opentelemetry.instrumentation.auto_instrumentation import (  # noqa: F401
        sitecustomize,
    )

    return provider


@warn_assertions
def initialise_job_trace(job):
    """Initialise the trace for this job by creating a root span.

    We store the serialised trace context in the db, so we can reuse it for
    later spans.

    We create a root span, which is a requirement in OTel. For this reason we
    send it out straight away, which means its duration is very short.
    """
    assert not job.trace_context, "this job already has a trace-context"
    assert job.status_code is not None, "job has no initial StatusCode"
    assert job.status_code_updated_at is not None, (
        "job has no initial status_code_updated_at"
    )

    job.trace_context = {}
    tracer = trace.get_tracer("jobs")

    # create a root span in order to have a parent for all subsequent spans.
    # However, we do not annotate or emit this span object now. We do this when
    # the job has completed; see complete_job() for details.
    root = tracer.start_span("JOB", context={})

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
    if not job.trace_context or job.status_code is None:
        logger.info(f"not tracing job {job.id} as not initialised for tracing")
        return False

    return True


def finish_current_job_state(
    job, timestamp_ns, exception=None, results=None, extra=None
):
    """Record a span representing the state we've just exited."""
    if not _traceable(job):  # pragma: no cover
        return

    try:
        name = job.status_code.name
        start_time = job.status_code_updated_at
        record_job_span(job, name, start_time, timestamp_ns, exception, results, extra)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace state for {job.id}")


def record_final_job_state(job, timestamp_ns, exception=None, results=None, extra=None):
    """Record a span representing the state we've just exited."""
    if not _traceable(job):  # pragma: no cover
        return

    try:
        name = job.status_code.name
        # Note: this *must* be timestamp as integer nanoseconds
        start_time = job.status_code_updated_at

        extra = {"job.succeeded": job.status_code == StatusCode.SUCCEEDED}

        # final states have no duration, so make last for 1 sec, just act
        # as a marker
        end_time = int(timestamp_ns + 1e9)
        record_job_span(
            job, name, start_time, end_time, exception, results, extra=extra
        )

        complete_job(job, timestamp_ns, exception, results, extra)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace state for {job.id}")


def load_root_span(job):
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

    return span_context


def load_trace_context(job):
    span_context = load_root_span(job)
    return propagation.set_span_in_context(trace.NonRecordingSpan(span_context), {})


MINIMUM_NS_TIMESTAMP = int(datetime(2000, 1, 1, 0, 0, 0).timestamp() * 1e9)


@warn_assertions
def record_job_span(job, name, start_time, end_time, exception, results, extra=None):
    """Record a span for a job."""
    if not _traceable(job):
        return

    # Due to @warn_assertions, this will be emitted as warnings in test, but
    # the calling code swallows any exceptions.
    assert start_time is not None
    assert end_time is not None
    assert start_time > MINIMUM_NS_TIMESTAMP, (
        f"start_time not in nanoseconds: {start_time}"
    )
    assert end_time > MINIMUM_NS_TIMESTAMP, f"end_time not in nanoseconds: {end_time}"
    # Note: timer precision can be low (e.g. on Windows), so we sometimes get the same
    # value of ns for two separate measurments. This means they are not always
    # increasing, but they should never decrease. At least in theory...
    assert end_time >= start_time, (
        f"end_time is before start_time, ({end_time} < {start_time})"
    )

    ctx = load_trace_context(job)
    tracer = trace.get_tracer("jobs")
    span = tracer.start_span(name, context=ctx, start_time=start_time)
    set_span_job_metadata(span, job, exception, results, extra)
    span.end(end_time)


def complete_job(job, timestamp_ns, exception=None, results=None, extra=None):
    """Send the root span to record the full duration for this job."""

    root_ctx = load_root_span(job)
    tracer = trace.get_tracer("jobs")

    # trace vanity: have the job start 1us before the actual job, so
    # it shows up first in the trace
    job_start_time = int(job.created_at * 1e9) - 1000

    # We created this root span at the start of the trace, as is required for
    # a trace, and every span has had it as its id as its parent span. However,
    # there is no easy way to serialize the actual span object now that we want
    # to send it.

    # this effectively starts a new trace
    root_span = tracer.start_span("JOB", context={}, start_time=job_start_time)

    # replace the context with the one from the original root span
    root_span._context = root_ctx

    # annotate and send
    set_span_job_metadata(root_span, job, exception, results, extra)
    root_span.end(timestamp_ns)


OTEL_ATTR_TYPES = (bool, str, bytes, int, float)


def set_span_job_metadata(span, job, exception=None, results=None, extra=None):
    """Set span metadata with everything we know about a job."""
    try:
        attributes = {}

        if extra:
            for k, v in extra.items():
                # automatically give any additional attributes the job prefix if not already present
                if not k.startswith("job."):  # pragma: nocover
                    # this will fail tests
                    warnings.warn(f"attribute {k} does not start with job. prefix")
                    # but correctly prefix it somehow this happens for real.
                    k = "job." + k
                attributes[k] = v

        attributes.update(trace_attributes(job, results))

        set_span_attributes(span, attributes)

        # temporary backwards compatibility, can remove after a few months
        set_span_attributes(span, backwards_compatible_job_attrs(attributes))

        if exception:
            span.record_exception(exception)

        # only internal errors are marked as span erros
        if job.status_code == StatusCode.INTERNAL_ERROR:
            if exception:
                msg = str(exception)
            else:  # pragma: nocover
                msg = "internal error"
            span.set_status(trace.Status(trace.StatusCode.ERROR, msg))

    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace job {job.id}")


BACKWARDS_MAPPING = {}


def backwards_compatible_job_attrs(attributes):
    bwcompat = {}

    for k, v in attributes.items():
        if not k.startswith("job."):
            continue

        if k == "job.id":
            k = "job"
        elif k == "job.request":
            k = "job_request"
        else:
            k = k.replace("job.", "")

        bwcompat[k] = v

    return bwcompat


def set_span_attributes(span, attributes):
    # opentelemetry can only handle serializing certain attribute types
    clean_attrs = {}
    for k, v in attributes.items():
        if not isinstance(v, OTEL_ATTR_TYPES):
            if v is not None:
                # log to help us notice this
                # values can often be None and this isn't particularly interesting, so don't fill up
                # the logs with that
                # If the span has no name attribute (e.g. NonRecordingSpans), log its type instead
                span_name = getattr(span, "name", type(span))
                logger.error(
                    f"Trace span {span_name} attribute {k} was set invalid type: {v}, type {type(v)}"
                )
                # coerce to string so we preserve some information
            v = str(v)
        clean_attrs[k] = v

    span.set_attributes(clean_attrs)


def trace_attributes(job, results=None):
    """These attributes are added to every span in order to slice and dice by
    each as needed.
    """
    # grab job request metadata, caching it on the job instance to avoid excess
    # queries/jsoning
    if job._job_request is None:
        job._job_request = get_saved_job_request(job)

    attrs = {
        "job.backend": job.backend,
        "job.id": job.id,
        "job.request": job.job_request_id,
        "job.workspace": job.workspace,
        "job.action": job.action,
        "job.commit": job.commit,
        "job.run_command": job.run_command,
        "job.user": job._job_request.get("created_by", "unknown"),
        "job.project": job._job_request.get("project", "unknown"),
        "job.orgs": ",".join(job._job_request.get("orgs", [])),
        "job.state": job.state.name,
        "job.message": job.status_message,
        # convert float seconds to ns integer
        "job.created_at": int(job.created_at * 1e9),
        "job.started_at": int(job.started_at * 1e9) if job.started_at else None,
        # when did the state last change?
        "job.status_code_updated_at": job.status_code_updated_at,
        "job.requires_db": job.requires_db,
    }

    if job.action_repo_url:
        attrs["job.reusable_action"] = job.action_repo_url
        if job.action_commit:  # pragma: no cover
            attrs["job.reusable_action"] += ":" + job.action_commit

    if results:
        attrs["job.exit_code"] = results.exit_code
        attrs["job.image_id"] = results.image_id
        attrs["job.executor_message"] = results.message
        attrs["job.action_version"] = results.action_version
        attrs["job.action_revision"] = results.action_revision
        attrs["job.action_created"] = results.action_created
        attrs["job.base_revision"] = results.base_revision
        attrs["job.base_created"] = results.base_created

    return attrs


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
        run_command="ehrql:latest cmd opt",
        commit="commit",
        created_at=timestamp,
    )
    initialise_job_trace(job)

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
