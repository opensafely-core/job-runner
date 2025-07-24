import os
import time

import opentelemetry.exporter.otlp.proto.http.trace_exporter
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import ConsoleSpanExporter

from common.tracing import setup_default_tracing, time_for_span
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


def test_setup_default_tracing_otlp_defaults(monkeypatch):
    env = {"OTEL_EXPORTER_OTLP_HEADERS": "'foo=bar'"}
    monkeypatch.setattr(os, "environ", env)
    monkeypatch.setattr(
        opentelemetry.exporter.otlp.proto.http.trace_exporter, "environ", env
    )
    provider = setup_default_tracing("test", set_global=False)
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
    provider = setup_default_tracing("test", set_global=False)
    assert provider.resource.attributes["service.name"] == "service"

    exporter = provider._active_span_processor._span_processors[0].span_exporter

    assert isinstance(exporter, OTLPSpanExporter)
    assert exporter._endpoint == "https://endpoint/v1/traces"
    assert exporter._headers == {"foo": "bar"}


def test_time_for_span_explicit_span():
    with tracer.start_as_current_span("test_span") as span:
        with time_for_span("block_duration_ms", span):
            time.sleep(0.01)

    spans = test_exporter.get_finished_spans()
    outer = next(s for s in spans if s.name == "test_span")
    assert "block_duration_ms" in outer.attributes
    # Attached time should be closed to the time we slept for.
    assert 0.02 >= outer.attributes["block_duration_ms"] >= 0.01


def test_time_for_span_current_span():
    with tracer.start_as_current_span("test_span"):
        # We don't pass span in explicitly, should default to it as current.
        with time_for_span("block_duration_ms"):
            time.sleep(0.01)

    spans = test_exporter.get_finished_spans()
    outer = next(s for s in spans if s.name == "test_span")
    assert "block_duration_ms" in outer.attributes
    # Attached time should be closed to the time we slept for.
    assert 0.02 >= outer.attributes["block_duration_ms"] >= 0.01
