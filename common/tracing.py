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

    # OTel client defaults are very low while Honeycomb supports much higher values. We
    # silently lose data if we hit these limits so we bump them here.
    #
    # Honeycomb limit: 2,000 fields per event
    os.environ.setdefault("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "2000")
    # Honeycomb limit: 64KB per string field (useful for formatted SQL queries)
    os.environ.setdefault("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT", "65536")
    # Use stable HTTP semantic conventions
    os.environ.setdefault("OTEL_SEMCONV_STABILITY_OPT_IN", "http")

    if "OTEL_EXPORTER_OTLP_HEADERS" in os.environ:
        # workaround for env file parsing issues
        cleaned_headers = os.environ["OTEL_EXPORTER_OTLP_HEADERS"].strip("\"'")
        # put back into env to be parsed properly
        os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = cleaned_headers

        if "OTEL_EXPORTER_OTLP_ENDPOINT" not in os.environ:
            os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://api.honeycomb.io"

    if "OTEL_EXPORTER_OTLP_ENDPOINT" in os.environ:
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


def set_span_attributes(span, attributes):
    # opentelemetry can only handle serializing certain attribute types
    clean_attrs = {}
    for k, v in attributes.items():
        clean_v = v
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
            clean_v = str(v)
        clean_attrs[k] = clean_v

    span.set_attributes(clean_attrs)


@contextmanager
def duration_ms_as_span_attr(attribute_name: str, span=None):
    """
    Context manager to time an operation and add it as an attribute to a span.

    Args:
        attribute_name: Name of the attribute to set on the span
        span: Optional span to add attribute to. If None, uses current span.
    """
    if span is None:
        span = trace.get_current_span()

    start_time = time.perf_counter_ns()
    try:
        yield
    finally:
        end_time = time.perf_counter_ns()
        # Convert duration to milliseconds
        duration_ms = int((end_time - start_time) / 1e6)
        span.set_attribute(attribute_name, duration_ms)
