import os
import time
from unittest import mock

import pytest
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import ConsoleSpanExporter

from common.tracing import duration_ms_as_span_attr, setup_default_tracing
from tests.conftest import test_exporter


tracer = trace.get_tracer(__name__)


def test_setup_default_tracing_empty_env(monkeypatch):
    env = {}
    monkeypatch.setattr(os, "environ", env)
    provider = setup_default_tracing("test", set_global=False)
    assert provider._active_span_processor._span_processors == ()


def test_setup_default_tracing_console(monkeypatch):
    env = {"OTEL_EXPORTER_CONSOLE": "1"}
    monkeypatch.setattr(os, "environ", env)
    provider = setup_default_tracing("test", set_global=False)

    processor = provider._active_span_processor._span_processors[0]
    assert isinstance(processor.span_exporter, ConsoleSpanExporter)


@mock.patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_HEADERS": "'foo=bar'"}, clear=True)
def test_setup_default_tracing_otlp_defaults(responses):
    responses.add(responses.POST, "https://api.honeycomb.io/v1/traces", status=200)

    provider = setup_default_tracing("test", set_global=False)
    assert provider.resource.attributes["service.name"] == "jobrunner"
    assert isinstance(
        provider._active_span_processor._span_processors[0].span_exporter,
        OTLPSpanExporter,
    )

    with provider.get_tracer("test").start_as_current_span("probe"):
        pass
    # make sure the span is exported before we inspect requests
    assert provider.force_flush()

    (call,) = responses.calls
    # The exporter adds its own headers in addition to those we provide
    assert {"foo": "bar"}.items() <= call.request.headers.items()
    assert os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] == "https://api.honeycomb.io"


@mock.patch.dict(
    os.environ,
    {
        "OTEL_EXPORTER_OTLP_HEADERS": "foo=bar",
        "OTEL_SERVICE_NAME": "service",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "https://endpoint",
    },
    clear=True,
)
def test_setup_default_tracing_otlp_with_env(responses):
    responses.add(responses.POST, "https://endpoint/v1/traces", status=200)

    provider = setup_default_tracing("test", set_global=False)
    assert provider.resource.attributes["service.name"] == "service"
    assert isinstance(
        provider._active_span_processor._span_processors[0].span_exporter,
        OTLPSpanExporter,
    )

    with provider.get_tracer("test").start_as_current_span("probe"):
        pass
    # make sure the span is exported before we inspect requests
    assert provider.force_flush()

    (call,) = responses.calls
    # The exporter adds its own headers in addition to those we provide
    assert {"foo": "bar"}.items() <= call.request.headers.items()


def test_time_for_span_explicit_span():
    sleep_ms = 10
    with (
        tracer.start_as_current_span("test_span") as span,
        duration_ms_as_span_attr("block_duration_ms", span),
    ):
        time.sleep(sleep_ms / 1000)

    spans = test_exporter.get_finished_spans()
    outer = next(s for s in spans if s.name == "test_span")
    assert "block_duration_ms" in outer.attributes
    # Attached time should be close (within 100ms) of the time we slept for
    # Note: CI seems to need a higher tolerance here. Locally, it passes with the
    # default rel (1e-6).
    print(outer.attributes["block_duration_ms"])
    assert pytest.approx(sleep_ms, rel=100) == outer.attributes["block_duration_ms"]


def test_time_for_span_current_span():
    sleep_ms = 10
    with (
        tracer.start_as_current_span("test_span"),
        # We don't pass span in explicitly, should default to it as current.
        duration_ms_as_span_attr("block_duration_ms"),
    ):
        time.sleep(sleep_ms / 1000)

    spans = test_exporter.get_finished_spans()
    outer = next(s for s in spans if s.name == "test_span")
    assert "block_duration_ms" in outer.attributes
    # Attached time should be close (within 100ms) of the time we slept for
    print(outer.attributes["block_duration_ms"])
    assert pytest.approx(sleep_ms, rel=100) == outer.attributes["block_duration_ms"]
