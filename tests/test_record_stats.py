import subprocess
import time

from jobrunner import record_stats
from jobrunner.models import State, StatusCode
from tests.conftest import get_trace
from tests.factories import job_factory


def test_record_tick_trace(db, freezer, monkeypatch):
    jobs = []
    jobs.append(job_factory(status_code=StatusCode.CREATED))
    jobs.append(job_factory(status_code=StatusCode.WAITING_ON_DEPENDENCIES))
    jobs.append(job_factory(status_code=StatusCode.PREPARING))
    running_job = job_factory(status_code=StatusCode.EXECUTING)
    jobs.append(running_job)
    jobs.append(job_factory(status_code=StatusCode.FINALIZING))

    metrics = {
        running_job.id: {
            "cpu_percentage": 50.0,
            "memory_used": 1000,
        }
    }

    monkeypatch.setattr(record_stats, "get_job_stats", lambda: metrics)

    # this should not be tick'd
    job_factory(state=State.SUCCEEDED, status_code=StatusCode.SUCCEEDED)

    last_run1 = record_stats.record_tick_trace(None)
    assert len(get_trace("ticks")) == 0

    freezer.tick(10)

    last_run2 = record_stats.record_tick_trace(last_run1)
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

        if job is running_job:
            assert span.attributes["cpu_percentage"] == 50.0
            assert span.attributes["memory_used"] == 1000

    assert "SUCCEEDED" not in [s.name for s in spans]


def test_record_tick_trace_stats_timeout(db, freezer, monkeypatch):
    job_factory(status_code=StatusCode.EXECUTING)

    def timeout():
        raise subprocess.TimeoutExpired("cmd", 10)

    monkeypatch.setattr(record_stats, "get_job_stats", timeout)

    last_run = time.time()
    freezer.tick(10)

    record_stats.record_tick_trace(last_run)
    assert len(get_trace("ticks")) == 2

    spans = get_trace("ticks")
    span = spans[0]

    assert "cpu_percentage" not in span.attributes
    assert "memory_used" not in span.attributes
