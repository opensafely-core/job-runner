import logging
import subprocess

from jobrunner import record_stats
from jobrunner.models import State, StatusCode
from tests.conftest import get_trace
from tests.factories import job_factory


def test_record_tick_trace(db, freezer):

    jobs = []
    jobs.append(job_factory(status_code=StatusCode.CREATED))
    jobs.append(job_factory(status_code=StatusCode.WAITING_ON_DEPENDENCIES))
    jobs.append(job_factory(status_code=StatusCode.PREPARING))
    jobs.append(job_factory(status_code=StatusCode.EXECUTING))
    jobs.append(job_factory(status_code=StatusCode.FINALIZING))

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
        assert span.attributes["tick"] is True
        assert span.attributes["job"] == job.id
        assert span.parent.span_id == root.context.span_id

    assert "SUCCEEDED" not in [s.name for s in spans]


def test_log_stats(db, caplog, monkeypatch):
    def error():
        raise subprocess.TimeoutExpired("test me", 10)

    caplog.set_level(logging.INFO)
    monkeypatch.setattr(record_stats, "get_all_stats", error)

    record_stats.log_stats(None)

    assert caplog.records[-1].msg == "Getting docker stats timed out"
    assert "test me" in caplog.records[-1].exc_text
