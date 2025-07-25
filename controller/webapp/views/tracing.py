from opentelemetry import trace

from common.tracing import set_span_attributes


def trace_attributes(**attrs):
    span = trace.get_current_span()
    set_span_attributes(span, attrs)
