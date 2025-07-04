import sqlite3
import subprocess
import time

import pytest

from agent import metrics
from jobrunner.config import agent as config
from jobrunner.job_executor import ExecutorState
from tests.agent.stubs import StubExecutorAPI
from tests.conftest import get_trace
from tests.factories import metrics_factory


def test_get_connection_readonly():
    conn = metrics.get_connection(readonly=True)
    assert conn is None

    conn = metrics.get_connection(readonly=False)
    assert conn is metrics.get_connection(readonly=False)  # cached
    assert conn.isolation_level is None
    assert conn.row_factory is sqlite3.Row
    assert conn.execute("PRAGMA journal_mode").fetchone()["journal_mode"] == "wal"
    assert (
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("jobs",)
        ).fetchone()["name"]
        == "jobs"
    )

    ro_conn = metrics.get_connection(readonly=True)
    assert ro_conn is metrics.get_connection(readonly=True)  # cached
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
    assert metrics.read_job_metrics("id") == {}

    # create db file
    sqlite3.connect(config.METRICS_FILE)

    # possible race condition, no table yet, should still report no metrics
    assert metrics.read_job_metrics("id") == {}

    metrics.write_job_metrics("id", {"test": 1.0})

    assert metrics.read_job_metrics("id") == {"test": 1.0}


@pytest.mark.parametrize("task_present", [True, False])
def test_record_metrics_tick_trace(
    db, freezer, monkeypatch, live_server, responses, task_present
):
    monkeypatch.setattr("jobrunner.config.agent.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    mb = 1024 * 1024

    if task_present:
        api = StubExecutorAPI()
        task, job_id = api.add_test_runjob_task(ExecutorState.EXECUTING)
        task_id = task.id
    else:
        job_id = "job_id"
        task_id = "unknown"

    started_at = int(time.time())  # frozen
    monkeypatch.setattr(
        metrics,
        "get_job_stats",
        lambda: {
            job_id: {
                "cpu_percentage": 50.0,
                "memory_used": 1000 * mb,
                "container_id": "a0b1c2d3",
                "started_at": started_at,
            }
        },
    )

    last_run1 = metrics.record_metrics_tick_trace(None)
    assert len(get_trace("metrics")) == 0

    freezer.tick(10)

    last_run2 = metrics.record_metrics_tick_trace(last_run1)
    assert last_run2 == last_run1 + 10 * 1e9

    spans = get_trace("metrics")
    assert len(spans) == 3  # root, single metric, and get_job_stats

    root = spans[-1]
    assert root.name == "METRICS_TICK"
    assert root.start_time == last_run1
    assert root.end_time == last_run2
    assert root.attributes["backend"] == "test"

    span = spans[-2]

    assert span.name == "METRICS"
    assert span.start_time == last_run1
    assert span.end_time == last_run2
    assert span.attributes["job"] == job_id
    assert span.attributes["task"] == task_id
    assert span.attributes["backend"] == "test"
    if task_present:
        assert span.attributes["user"] == "testuser"
        assert span.attributes["project"] == "project"
        assert span.attributes["orgs"] == "org1,org2"
    else:
        for key in ["user", "project", "orgs"]:
            assert key not in span.attributes
    assert span.parent.span_id == root.context.span_id

    assert span.attributes["stats_timeout"] is False
    assert span.attributes["stats_error"] is False

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

    span = spans[-3]
    assert span.name == "get_job_stats"


def test_record_metrics_tick_trace_stats_timeout(
    db, freezer, live_server, responses, monkeypatch
):
    monkeypatch.setattr("jobrunner.config.agent.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    def timeout():
        raise subprocess.TimeoutExpired("cmd", 10)

    monkeypatch.setattr(metrics, "get_job_stats", timeout)

    last_run = time.time()
    freezer.tick(10)

    metrics.record_metrics_tick_trace(last_run)
    assert len(get_trace("metrics")) == 2

    spans = get_trace("metrics")
    span = spans[-1]

    assert "cpu_percentage" not in span.attributes
    assert "memory_used" not in span.attributes
    assert "mem_mb_peak" not in span.attributes
    assert span.attributes["stats_timeout"] is True
    assert span.attributes["stats_error"] is False

    span = spans[-2]
    assert span.name == "get_job_stats"


def test_record_metrics_tick_trace_stats_error(
    db, freezer, monkeypatch, live_server, responses
):
    monkeypatch.setattr("jobrunner.config.agent.TASK_API_ENDPOINT", live_server.url)
    responses.add_passthru(live_server.url)

    def error():
        raise subprocess.CalledProcessError(
            returncode=1, cmd=["test", "cmd"], output="stdout", stderr="stderr"
        )

    monkeypatch.setattr(metrics, "get_job_stats", error)

    last_run = time.time()
    metrics.record_metrics_tick_trace(last_run)
    assert len(get_trace("metrics")) == 2

    spans = get_trace("metrics")
    span = spans[-1]

    assert "cpu_percentage" not in span.attributes
    assert "memory_used" not in span.attributes
    assert "mem_mb_peak" not in span.attributes
    assert span.attributes["stats_timeout"] is False
    assert span.attributes["stats_error"] is True
    span = spans[-2]
    assert span.name == "get_job_stats"


def test_update_job_metrics(db):
    job_id = "job_id"
    metrics_factory(job_id)

    assert metrics.read_job_metrics(job_id) == {}
    mb = 1024.0 * 1024.0

    # 50%/100m for 1s
    metrics.update_job_metrics(
        job_id,
        {
            "cpu_percentage": 50,
            "memory_used": 100 * mb,
            "container_id": "a0b1c2d3",
        },
        duration_s=1.0,
        runtime_s=1.0,
    )

    assert metrics.read_job_metrics(job_id) == {
        "cpu_cumsum": 50.0,
        "cpu_mean": 50.0,
        "cpu_peak": 50,
        "cpu_sample": 50,
        "mem_mb_cumsum": 100.0,
        "mem_mb_mean": 100.0,
        "mem_mb_peak": 100,
        "mem_mb_sample": 100,
        "container_id": "a0b1c2d3",
    }

    # 100%/1000m for 1s
    metrics.update_job_metrics(
        job_id,
        {
            "cpu_percentage": 100,
            "memory_used": 1000 * mb,
            "container_id": "a0b1c2d3",
        },
        duration_s=1.0,
        runtime_s=2.0,
    )

    assert metrics.read_job_metrics(job_id) == {
        "cpu_cumsum": 150.0,
        "cpu_mean": 75.0,
        "cpu_peak": 100,
        "cpu_sample": 100,
        "mem_mb_cumsum": 1100.0,
        "mem_mb_mean": 550.0,
        "mem_mb_peak": 1000,
        "mem_mb_sample": 1000,
        "container_id": "a0b1c2d3",
    }

    # 100%/1000m for 8s
    metrics.update_job_metrics(
        job_id,
        {
            "cpu_percentage": 100,
            "memory_used": 1000 * mb,
            "container_id": "a0b1c2d3",
        },
        duration_s=8.0,
        runtime_s=10.0,
    )

    assert metrics.read_job_metrics(job_id) == {
        "cpu_cumsum": 950.0,
        "cpu_mean": 95.0,
        "cpu_peak": 100,
        "cpu_sample": 100,
        "mem_mb_cumsum": 9100.0,
        "mem_mb_mean": 910.0,
        "mem_mb_peak": 1000,
        "mem_mb_sample": 1000,
        "container_id": "a0b1c2d3",
    }

    # Job has been restarted (note reset `runtime_s` and new container_id)
    metrics.update_job_metrics(
        job_id,
        {
            "cpu_percentage": 50,
            "memory_used": 100 * mb,
            "container_id": "e4f5a6b7",
        },
        duration_s=1.0,
        runtime_s=1.0,
    )

    # Metrics should be reset as a result of the container_id changing
    assert metrics.read_job_metrics(job_id) == {
        "cpu_cumsum": 50.0,
        "cpu_mean": 50.0,
        "cpu_peak": 50,
        "cpu_sample": 50,
        "mem_mb_cumsum": 100.0,
        "mem_mb_mean": 100.0,
        "mem_mb_peak": 100,
        "mem_mb_sample": 100,
        "container_id": "e4f5a6b7",
    }
