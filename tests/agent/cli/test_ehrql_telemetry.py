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


ehrql_log = """
2025-07-01T16:56:30.687958737Z [info   ] Compiling dataset definition from analysis/dataset_definition/dataset_definition_vax.py
2025-07-01T16:56:46.227330349Z [info   ] Generating dataset
2025-07-01T16:57:00.993809188Z [info   ] Running query 001 / 331
2025-07-01T16:57:00.995883302Z [info   ] SQL:
2025-07-01T16:57:00.995931727Z           SELECT * INTO [#tmp_1] FROM (SELECT patients.patient_id AS patient_id
2025-07-01T16:57:00.995952045Z           FROM (
2025-07-01T16:57:00.995970602Z                       SELECT
2025-07-01T16:57:00.995988488Z                           Patient_ID as patient_id,
2025-07-01T16:57:00.996005352Z                           DateOfBirth as date_of_birth,
2025-07-01T16:57:00.996023614Z                           CASE
2025-07-01T16:57:00.996043264Z                               WHEN Sex = 'M' THEN 'male' COLLATE Latin1_General_CI_AS
2025-07-01T16:57:00.996061649Z                               WHEN Sex = 'F' THEN 'female' COLLATE Latin1_General_CI_AS
2025-07-01T16:57:00.996210794Z                               WHEN Sex = 'I' THEN 'intersex' COLLATE Latin1_General_CI_AS
2025-07-01T16:57:00.996234454Z                               ELSE 'unknown' COLLATE Latin1_General_CI_AS
2025-07-01T16:57:00.996252317Z                           END AS sex,
2025-07-01T16:57:00.996265184Z                           NULLIF(DateOfDeath, '99991231') AS date_of_death
2025-07-01T16:57:00.996277761Z                       FROM Patient
2025-07-01T16:57:00.996290041Z                   ) AS patients UNION SELECT [PatientsWithTypeOneDissent].[Patient_ID] AS patient_id
2025-07-01T16:57:00.996302988Z           FROM [PatientsWithTypeOneDissent]) AS anon_1
2025-07-01T16:57:01.010057734Z [info   ] scans logical physical read_ahead lob_logical lob_physical lob_read_ahead table
2025-07-01T16:57:01.010114642Z           1     0       0        0          0           0            0              PatientsWithTypeOneDissent
2025-07-01T16:57:01.010136538Z           1     0       0        0          0           0            0              Patient
2025-07-01T16:57:01.010240821Z [info   ] 0 seconds: exec_cpu_ms=3 exec_elapsed_ms=3 exec_cpu_ratio=1.0 parse_cpu_ms=0 parse_elapsed_ms=0 query_id=query 001 / 331
"""


# this is not run by CI by default, as it is slow and brittle, hence the no-cover
# run manually with just test -o python_functions=manual_test
def manual_test_convert_ehrql_logs(tmp_path):  # pragma: nocover
    logfile = tmp_path / "logs.txt"
    logfile.write_text(ehrql_log)

    lines = ehrql_telemetry.convert_ehrql_logs(logfile)
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert "name" in data
    assert "start" in data
    assert "end" in data
    assert "attributes" in data
