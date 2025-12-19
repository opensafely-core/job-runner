import json

from agent.cli import ehrql_log_telemetry
from tests.conftest import get_trace


def logdata(name="name", start=None, end=None, events=None, **kwargs):
    """Minimal log data"""
    return {
        "name": name,
        "start": start or "2025-07-02T10:23:45.123456789Z",
        "end": end or "2025-07-02T12:23:45.123456789Z",
        "attributes": kwargs,
        **({"events": events} if events else {}),
    }


def set_logs(monkeypatch, lines):
    monkeypatch.setattr(ehrql_log_telemetry, "convert_ehrql_logs", lambda _: lines)


def test_ehrql_log_telemetry_run():
    log = logdata(test=True)
    ehrql_log_telemetry.run([log], "generate-dataset", {"foo": "bar"})
    spans = get_trace()
    assert spans[0].name == log["name"]
    assert spans[0].start_time == ehrql_log_telemetry.docker_datestr_to_ns(log["start"])
    assert spans[0].end_time == ehrql_log_telemetry.docker_datestr_to_ns(log["end"])
    assert spans[0].attributes["test"]

    assert spans[1].name == "ehrql.generate-dataset"

    for span in spans:
        assert span.attributes["foo"] == "bar"


def test_ehrql_telemetry_run_with_events():
    exc_event = {
        "name": "exception",
        "timestamp": "2025-09-08T16:41:37.837685167Z",
        "attributes": {
            "exception.type": "pymssql._mssql.MSSQLDatabaseException",
            "exception.message": "(20047, b'DBPROCESS is dead or not enabled')",
            "exception.stacktrace": " ... full stacktrace as text ... ",
        },
    }
    log = logdata(test=True, events=[exc_event])
    ehrql_log_telemetry.run([log], "generate-dataset", {})
    spans = get_trace()
    assert len(spans[0].events) == 1
    assert len(spans[1].events) == 0

    event = spans[0].events[0]
    assert event.name == exc_event["name"]
    assert event.timestamp == ehrql_log_telemetry.docker_datestr_to_ns(
        exc_event["timestamp"]
    )
    assert event.attributes == exc_event["attributes"]


def test_ehrql_telemetry_main_with_dir(tmp_path, monkeypatch):
    set_logs(
        monkeypatch,
        [
            json.dumps(logdata(name="first")),
            json.dumps(logdata(name="second")),
        ],
    )

    ehrql_log_telemetry.main(
        [
            "dataset",
            str(tmp_path),
            "generate-dataset",
            "workspace",
            "abcde",
            "action1",
            "--attrs",
            "apply-filtering=True",
            "foo=bar",
        ]
    )
    spans = get_trace()
    assert spans[0].name == "first"
    assert spans[1].name == "second"
    assert spans[2].name == "ehrql.generate-dataset"

    for span in spans:
        span.attributes["workspace"] == "workspace"
        span.attributes["commit"] == "workspace"
        span.attributes["workspace"] == "adcde"
        span.attributes["action"] == "action1"
        span.attributes["apply-filtering"] == "True"
        span.attributes["foo"] == "bar"


def test_ehrql_telemetry_main_bad_json_line(tmp_path, monkeypatch):
    set_logs(
        monkeypatch,
        [
            json.dumps(logdata(name="first")),
            "{[}",
            "",
            json.dumps(logdata(name="second")),
        ],
    )

    ehrql_log_telemetry.main(
        [
            "dataset",
            str(tmp_path),
            "generate-dataset",
            "workspace",
            "abcde",
            "action1",
        ]
    )
    spans = get_trace()
    assert spans[0].name == "first"
    assert spans[1].name == "second"
    assert spans[2].name == "ehrql.generate-dataset"
