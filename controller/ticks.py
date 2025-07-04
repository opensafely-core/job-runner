"""
Super crude docker/system stats logger
"""

import logging
import sys
import time

from opentelemetry import trace

from common.lib.log_utils import configure_logging
from controller import config, models
from controller.lib import database
from jobrunner import tracing


log = logging.getLogger(__name__)
tracer = trace.get_tracer("ticks")


def main():  # pragma: no cover
    last_run = None
    while True:
        before = time.time()
        active_jobs = database.find_where(
            models.Job, state__in=[models.State.PENDING, models.State.RUNNING]
        )
        last_run = record_job_tick_trace(last_run, active_jobs)

        # record_tick_trace might have take a while, so sleep the remaining interval.
        # Enforce a minimum time of 2s to ensure we don't hammer honeycomb
        elapsed = time.time() - before
        time.sleep(max(2, config.TICK_POLL_INTERVAL - elapsed))


def record_job_tick_trace(last_run, active_jobs):
    """Record a period tick trace of current jobs.

    This will give us more realtime information than the job traces, which only
    send spans data when *leaving* a state.

    The easiest way to filter these in honeycomb is on the attribute `scope = ticks`

    Note that this will emit number of active jobs + 1 events every call, so we
    don't want to call it on too tight a loop.
    """

    if last_run is None:
        return time.time_ns()

    # record time once stats call has completed, as it can take a while
    now = time.time_ns()

    # every child span has the same timings
    start_time = last_run
    end_time = now

    with tracer.start_as_current_span("TICK", start_time=start_time):
        for job in active_jobs:
            # record span with clamped start/end times
            span = tracer.start_span(job.status_code.name, start_time=start_time)
            tracing.set_span_job_metadata(span, job)
            span.end(end_time)

    return end_time


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
