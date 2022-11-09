import time

from jobrunner import record_stats
from jobrunner.models import State, StatusCode
from tests.conftest import get_trace
from tests.factories import job_factory


def test_record_tick_trace(db):

    jobs = []
    jobs.append(job_factory(status_code=StatusCode.CREATED))
    jobs.append(job_factory(status_code=StatusCode.WAITING_ON_DEPENDENCIES))
    jobs.append(job_factory(status_code=StatusCode.PREPARING))
    jobs.append(job_factory(status_code=StatusCode.EXECUTING))
    jobs.append(job_factory(status_code=StatusCode.FINALIZING))

    # this should not be tick'd
    job_factory(state=State.SUCCEEDED, status_code=StatusCode.SUCCEEDED)

    last_run = int((time.time() - 10) * 1e9)
    record_stats.record_tick_trace(last_run)

    spans = get_trace()

    end_time = spans[0].end_time

    root = spans[-1]
    assert root.name == "TICK"
    assert root.start_time == last_run

    for job, span in zip(jobs, spans):
        assert span.name == job.status_code.name
        assert span.start_time == last_run
        assert span.end_time == end_time
        assert span.attributes["tick"] is True
        assert span.attributes["job"] == job.id
        assert span.parent.span_id == root.context.span_id

    assert "SUCCEEDED" not in [s.name for s in spans]


def test_record_tick_trace_last_run_is_none(db):
    now = int((time.time() - 10) * 1e9)
    last_run = record_stats.record_tick_trace(None)
    assert last_run > now
    assert len(get_trace()) == 0
