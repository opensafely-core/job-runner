import logging
import os
import time
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from common import config as common_config


logger = logging.getLogger(__name__)


def get_provider(service):
    # https://github.com/open-telemetry/semantic-conventions/tree/main/docs/resource#service
    resource = Resource.create(
        attributes={
            "service.name": os.environ.get("OTEL_SERVICE_NAME", "jobrunner"),
            # Note this will be set in the agent only
            "service.namespace": os.environ.get("BACKEND", "unknown"),
            "service.version": common_config.VERSION,
            "rap.service": service,
            "rap.backend": os.environ.get("BACKEND", "unknown"),
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


def setup_default_tracing(service, set_global=True):
    """Inspect environment variables and set up exporters accordingly."""

    provider = get_provider(service)

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


OTEL_ATTR_TYPES = (bool, str, bytes, int, float)


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


@contextmanager
def time_for_span(attribute_name: str, span=None):
    """
    Context manager to time an operation and add it as an attribute to a span.

    Args:
        attribute_name: Name of the attribute to set on the span
        span: Optional span to add attribute to. If None, uses current span.
    """
    if span is None:
        span = trace.get_current_span()

    start_time = time.perf_counter()
    try:
        yield
    finally:
        end_time = time.perf_counter()
        duration = end_time - start_time
        span.set_attribute(attribute_name, duration)
