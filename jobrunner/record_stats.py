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

    trace_attrs = {"stats_timeout": False, "stats_error": False}
    stats = {}
    error_attrs = {}

    try:
        stats = get_job_stats()
    except subprocess.TimeoutExpired:
        log.exception("Getting docker stats timed out")
        trace_attrs["stats_timeout"] = True
    except subprocess.CalledProcessError as exc:
        log.exception("Error getting docker stats")
        trace_attrs["stats_error"] = True

        error_attrs["cmd"] = " ".join(exc.cmd)
        error_attrs["exit_code"] = exc.returncode
        error_attrs["output"] = exc.stderr + "\n\n" + exc.output

    # record time once stats call has completed, as it can take a while
    now = time.time_ns()

    # every span has the same timings
    start_time = last_run
    end_time = now

    active_jobs = database.find_where(
        models.Job, state__in=[models.State.PENDING, models.State.RUNNING]
    )

    with tracer.start_as_current_span(
        "TICK", start_time=start_time, attributes=trace_attrs
    ) as root:
        # add error event so we can see the error from the docker command
        if error_attrs:
            root.add_event("stats_error", attributes=error_attrs, timestamp=start_time)

        for job in active_jobs:
            span = tracer.start_span(job.status_code.name, start_time=start_time)

            # set up attributes
            job_span_attrs = {}
            job_span_attrs.update(trace_attrs)
            metrics = stats.get(job.id, {})
            job_span_attrs["has_metrics"] = metrics != {}
            job_span_attrs.update(metrics)

            # record span
            tracing.set_span_metadata(span, job, **job_span_attrs)
            span.end(end_time)

    return end_time


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
