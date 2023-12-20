import sqlite3
import subprocess
import time

from jobrunner import config, record_stats
from jobrunner.models import State, StatusCode
from tests.conftest import get_trace
from tests.factories import job_factory, metrics_factory


def test_get_connection_readonly():
    conn = record_stats.get_connection(readonly=True)
    assert conn is None

    conn = record_stats.get_connection(readonly=False)
    assert conn is record_stats.get_connection(readonly=False)  # cached
    assert conn.isolation_level is None
    assert conn.row_factory is sqlite3.Row
    assert conn.execute("PRAGMA journal_mode").fetchone()["journal_mode"] == "wal"
    assert (
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("jobs",)
        ).fetchone()["name"]
        == "jobs"
    )

    ro_conn = record_stats.get_connection(readonly=True)
    assert ro_conn is record_stats.get_connection(readonly=True)  # cached
    assert ro_conn is not conn
    assert conn.isolation_level is None
    assert conn.row_factory is sqlite3.Row
    assert conn.execute("PRAGMA journal_mode").fetchone()["journal_mode"] == "wal"
    assert (
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("jobs",)
        ).fetchone()["name"]
        == "jobs"
    )


def test_read_write_job_metrics():

    assert record_stats.read_job_metrics("id") == {}

    # create db file
    sqlite3.connect(config.METRICS_FILE)

    # possible race condition, no table yet, should still report no metrics
    assert record_stats.read_job_metrics("id") == {}

    record_stats.write_job_metrics("id", {"test": 1.0})

    assert record_stats.read_job_metrics("id") == {"test": 1.0}


def test_record_tick_trace(db, freezer, monkeypatch):
    jobs = []
    jobs.append(job_factory(status_code=StatusCode.CREATED))
    jobs.append(job_factory(status_code=StatusCode.WAITING_ON_DEPENDENCIES))
    jobs.append(job_factory(status_code=StatusCode.PREPARING))
    running_job = job_factory(status_code=StatusCode.EXECUTING)
    jobs.append(running_job)
    jobs.append(job_factory(status_code=StatusCode.FINALIZING))

    mb = 1024 * 1024
    metrics = {
        running_job.id: {
            "cpu_percentage": 50.0,
            "memory_used": 1000 * mb,
        }
    }

    monkeypatch.setattr(record_stats, "get_job_stats", lambda: metrics)

    # this should not be tick'd
    job_factory(state=State.SUCCEEDED, status_code=StatusCode.SUCCEEDED)

    last_run1 = record_stats.record_tick_trace(None, jobs)
    assert len(get_trace("ticks")) == 0

    freezer.tick(10)

    last_run2 = record_stats.record_tick_trace(last_run1, jobs)
    assert last_run2 == last_run1 + 10 * 1e9

    spans = get_trace("ticks")

    root = spans[-1]
    assert root.name == "TICK"
    assert root.start_time == last_run1
    assert root.end_time == last_run2

    for job, span in zip(jobs, spans):
        assert span.name == job.status_code.name
        assert span.start_time == last_run1
        assert span.end_time == last_run2
        assert span.attributes["job"] == job.id
        assert span.parent.span_id == root.context.span_id

        assert span.attributes["stats_timeout"] is False
        assert span.attributes["stats_error"] is False

        if job is running_job:
            assert span.attributes["has_metrics"] is True
            assert span.attributes["cpu_percentage"] == 50.0
            assert span.attributes["cpu_sample"] == 50.0
            assert span.attributes["cpu_sample"] == 50.0
            assert span.attributes["cpu_peak"] == 50.0
            assert span.attributes["cpu_cumsum"] == 500.0  # 50% * 10s
            assert span.attributes["cpu_mean"] == 50.0

            assert span.attributes["memory_used"] == 1000 * mb
            assert span.attributes["mem_mb_sample"] == 1000
            assert span.attributes["mem_mb_peak"] == 1000
            assert span.attributes["mem_mb_cumsum"] == 10000  # 1000 * 10s
            assert span.attributes["mem_mb_mean"] == 1000
        else:
            assert span.attributes["has_metrics"] is False

    assert "SUCCEEDED" not in [s.name for s in spans]


def test_record_tick_trace_stats_timeout(db, freezer, monkeypatch):
    job = job_factory(status_code=StatusCode.EXECUTING)

    def timeout():
        raise subprocess.TimeoutExpired("cmd", 10)

    monkeypatch.setattr(record_stats, "get_job_stats", timeout)

    last_run = time.time()
    freezer.tick(10)

    record_stats.record_tick_trace(last_run, [job])
    assert len(get_trace("ticks")) == 2

    spans = get_trace("ticks")
    span = spans[0]

    assert "cpu_percentage" not in span.attributes
    assert "memory_used" not in span.attributes
    assert "mem_mb_peak" not in span.attributes
    assert span.attributes["has_metrics"] is False
    assert span.attributes["stats_timeout"] is True
    assert span.attributes["stats_error"] is False


def test_record_tick_trace_stats_error(db, freezer, monkeypatch):
    job = job_factory(status_code=StatusCode.EXECUTING)

    def error():
        raise subprocess.CalledProcessError(
            returncode=1, cmd=["test", "cmd"], output="stdout", stderr="stderr"
        )

    monkeypatch.setattr(record_stats, "get_job_stats", error)

    last_run = time.time()
    record_stats.record_tick_trace(last_run, [job])
    assert len(get_trace("ticks")) == 2

    spans = get_trace("ticks")
    span = spans[0]

    assert "cpu_percentage" not in span.attributes
    assert "memory_used" not in span.attributes
    assert "mem_mb_peak" not in span.attributes
    assert span.attributes["has_metrics"] is False
    assert span.attributes["stats_timeout"] is False
    assert span.attributes["stats_error"] is True

    root = spans[1]
    assert root.attributes["stats_timeout"] is False
    assert root.attributes["stats_error"] is True
    assert root.events[0].attributes["exit_code"] == 1
    assert root.events[0].attributes["cmd"] == "test cmd"
    assert root.events[0].attributes["output"] == "stderr\n\nstdout"
    assert root.events[0].name == "stats_error"


def test_update_job_metrics(db):

    job = job_factory(status_code=StatusCode.EXECUTING)
    metrics_factory(job)

    metrics = record_stats.read_job_metrics(job.id)

    assert metrics == {}
    mb = 1024.0 * 1024.0

    # 50%/100m for 1s
    record_stats.update_job_metrics(
        job,
        {"cpu_percentage": 50, "memory_used": 100 * mb},
        duration_s=1.0,
        runtime_s=1.0,
    )

    metrics = record_stats.read_job_metrics(job.id)
    assert metrics == {
        "cpu_cumsum": 50.0,
        "cpu_mean": 50.0,
        "cpu_peak": 50,
        "cpu_sample": 50,
        "mem_mb_cumsum": 100.0,
        "mem_mb_mean": 100.0,
        "mem_mb_peak": 100,
        "mem_mb_sample": 100,
    }

    # 100%/1000m for 1s
    record_stats.update_job_metrics(
        job,
        {"cpu_percentage": 100, "memory_used": 1000 * mb},
        duration_s=1.0,
        runtime_s=2.0,
    )

    metrics = record_stats.read_job_metrics(job.id)
    assert metrics == {
        "cpu_cumsum": 150.0,
        "cpu_mean": 75.0,
        "cpu_peak": 100,
        "cpu_sample": 100,
        "mem_mb_cumsum": 1100.0,
        "mem_mb_mean": 550.0,
        "mem_mb_peak": 1000,
        "mem_mb_sample": 1000,
    }

    # 100%/1000m for 8s
    record_stats.update_job_metrics(
        job,
        {"cpu_percentage": 100, "memory_used": 1000 * mb},
        duration_s=8.0,
        runtime_s=10.0,
    )

    metrics = record_stats.read_job_metrics(job.id)
    assert metrics == {
        "cpu_cumsum": 950.0,
        "cpu_mean": 95.0,
        "cpu_peak": 100,
        "cpu_sample": 100,
        "mem_mb_cumsum": 9100.0,
        "mem_mb_mean": 910.0,
        "mem_mb_peak": 1000,
        "mem_mb_sample": 1000,
    }
