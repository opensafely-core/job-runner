import json

import pytest

from agent.cli import ehrql_telemetry
from agent.executors import local
from tests.conftest import get_trace


def metadata(jobid="job_id"):
    """Minimal metadata.json contents needed for telemetry"""
    return {
        "job_definition_id": jobid,
        "task_id": "task_id",
        "exit_code": "0",
        "oom_killed": False,
        "cancelled": False,
        "error": False,
        "workspace": "workspace",
        "job_metrics": {
            "cpu_peak": 0,
            "cpu_mean": 0,
            "mem_mb_peak": 0,
            "mem_mb_mean": 0,
        },
        "container_metadata": {
            "State": {
                "StartedAt": "2025-07-02T10:23:45.123456789Z",
                "FinishedAt": "2025-07-02T12:23:45.123456789Z",
            },
            "Args": ["foo", "bar"],
            "Config": {
                "Image": "ghcr.io/opensafely-core/ehrql:v1",
                "Labels": {"action": "action", "workspace": "workspace"},
            },
        },
    }


def logdata(name="name", start=None, end=None, **kwargs):
    """Minimal log data"""
    return {
        "name": name,
        "start": start or "2025-07-02T10:23:45.123456789Z",
        "end": end or "2025-07-02T12:23:45.123456789Z",
        "attributes": kwargs,
    }


def set_logs(monkeypatch, lines):
    monkeypatch.setattr(ehrql_telemetry, "convert_ehrql_logs", lambda _: lines)


def test_ehrql_telemetry_run():
    meta = metadata()
    log = logdata(test=True)
    ehrql_telemetry.run(meta, [log])
    spans = get_trace()
    assert spans[0].name == log["name"]
    assert spans[0].start_time == ehrql_telemetry.docker_datestr_to_ns(log["start"])
    assert spans[0].end_time == ehrql_telemetry.docker_datestr_to_ns(log["end"])
    assert spans[0].attributes["test"]

    assert spans[1].name == "ehrql.foo"

    attrs = ehrql_telemetry.get_attrs(meta)
    for span in spans:
        for k, v in attrs.items():
            assert span.attributes[k] == v


def test_ehrql_telemetry_run_no_end_timestamp():
    meta = metadata()
    log = logdata(test=True)
    log["end"] = None
    ehrql_telemetry.run(meta, [log])
    spans = get_trace()
    assert spans[0].name == log["name"]
    assert spans[0].start_time == ehrql_telemetry.docker_datestr_to_ns(log["start"])
    assert spans[0].end_time == ehrql_telemetry.docker_datestr_to_ns(
        meta["container_metadata"]["State"]["FinishedAt"]
    )
    assert spans[0].attributes["test"]

    assert spans[1].name == "ehrql.foo"

    attrs = ehrql_telemetry.get_attrs(meta)
    for span in spans:
        for k, v in attrs.items():
            assert span.attributes[k] == v


def test_ehrql_telemetry_main_with_dir(tmp_path, monkeypatch):
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata()))
    set_logs(
        monkeypatch,
        [
            json.dumps(logdata(name="first")),
            json.dumps(logdata(name="second")),
        ],
    )

    ehrql_telemetry.main(["dataset", str(tmp_path)])
    spans = get_trace()
    assert spans[0].name == "first"
    assert spans[1].name == "second"
    assert spans[2].name == "ehrql.foo"


def test_ehrql_telemetry_main_with_id(monkeypatch, tmp_path):
    monkeypatch.setattr(local.config, "JOB_LOG_DIR", tmp_path)
    job_id = "1234"
    meta = metadata(job_id)
    logdir = local.get_log_dir(job_id)
    logdir.mkdir(parents=True)

    metadata_path = logdir / "metadata.json"
    metadata_path.write_text(json.dumps(meta))
    set_logs(
        monkeypatch,
        [
            json.dumps(logdata(name="first")),
            json.dumps(logdata(name="second")),
        ],
    )

    ehrql_telemetry.main(["dataset", job_id])
    spans = get_trace()
    assert spans[0].name == "first"
    assert spans[1].name == "second"
    assert spans[2].name == "ehrql.foo"


def test_ehrql_telemetry_main_bad_json_line(tmp_path, monkeypatch):
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata()))
    set_logs(
        monkeypatch,
        [
            json.dumps(logdata(name="first")),
            "{[}",
            "",
            json.dumps(logdata(name="second")),
        ],
    )

    ehrql_telemetry.main(["dataset", str(tmp_path)])
    spans = get_trace()
    assert spans[0].name == "first"
    assert spans[1].name == "second"
    assert spans[2].name == "ehrql.foo"


def test_ehrql_telemetry_main_non_ehrql_job(tmp_path):
    metadata_path = tmp_path / "metadata.json"
    meta = metadata()
    meta["container_metadata"]["Config"]["Image"] = "something else"
    metadata_path.write_text(json.dumps(meta))

    with pytest.raises(AssertionError, match="non-ehrql"):
        ehrql_telemetry.main(["dataset", str(tmp_path)])
