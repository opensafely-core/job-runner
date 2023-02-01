"""
Super crude docker/system stats logger
"""
import logging
import subprocess
import sys
import time

from opentelemetry import trace

from jobrunner import config, models, tracing
from jobrunner.lib import database
from jobrunner.lib.docker_stats import get_job_stats
from jobrunner.lib.log_utils import configure_logging


log = logging.getLogger(__name__)
tracer = trace.get_tracer("ticks")


def main():
    last_run = None
    while True:
        before = time.time()
        last_run = record_tick_trace(last_run)

        # record_tick_trace might have take a while, so sleep the remainding interval
        # enforce a minimum time of 3s to ensure we don't hammer honeycomb or
        # the docker api
        elapsed = time.time() - before
        time.sleep(max(2, config.STATS_POLL_INTERVAL - elapsed))


def record_tick_trace(last_run):
    """Record a period tick trace of current jobs.

    This will give us more realtime information than the job traces, which only
    send spans data when *leaving* a state.

    The easiest way to filter these in honeycomb is on tick==true attribute

    Not that this will emit number of active jobs + 1 events every call, so we
    don't want to call it on too tight a loop.
    """

    if last_run is None:
        return time.time_ns()

    try:
        stats = get_job_stats()
    except subprocess.TimeoutExpired:
        log.exception("Getting docker stats timed out")
        # no metrics for this tick
        stats = {}

    # record time once stats call has completed, as it can take a while
    now = time.time_ns()

    # every span has the same timings
    start_time = last_run
    end_time = now

    active_jobs = database.find_where(
        models.Job, state__in=[models.State.PENDING, models.State.RUNNING]
    )

    with tracer.start_as_current_span("TICK", start_time=start_time):
        for job in active_jobs:
            span = tracer.start_span(job.status_code.name, start_time=start_time)
            metrics = stats.get(job.id, {})
            tracing.set_span_metadata(span, job, **metrics)
            span.end(end_time)

    return end_time


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
