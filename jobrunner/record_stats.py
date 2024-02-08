"""
Super crude docker/system stats logger
"""
import json
import logging
import sqlite3
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

from opentelemetry import trace

from jobrunner import config, models, tracing
from jobrunner.lib import database
from jobrunner.lib.docker_stats import get_job_stats
from jobrunner.lib.log_utils import configure_logging


log = logging.getLogger(__name__)
tracer = trace.get_tracer("ticks")

# Simplest possible table. We're only storing aggregate data
DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT,
    metrics TEXT,
    PRIMARY KEY (id)
)
"""

CONNECTION_CACHE = threading.local()


def get_connection(readonly=True):
    db_file = config.METRICS_FILE

    # developer check against using memory dbs, which cannot be used with this
    # function, as we need to set mode ourselves
    assert isinstance(db_file, Path), "config.METRICS_FILE db must be file path"
    assert not str(db_file).startswith(
        "file:"
    ), "config.METRICS_FILE db must be file path, not url"

    if readonly:
        db = f"file:{db_file}?mode=ro"
    else:
        db = f"file:{db_file}?mode=rwc"

    cache = CONNECTION_CACHE.__dict__
    if db not in cache:
        try:
            conn = sqlite3.connect(db, uri=True)
        except sqlite3.OperationalError as exc:
            # if its readonly, we cannot create file, so fail gracefully.
            # Caller should check for conn being None.
            if readonly and "unable to open" in str(exc).lower():
                return None
            raise

        # manual transactions
        conn.isolation_level = None
        # Support dict-like access to rows
        conn.row_factory = sqlite3.Row

        if not readonly:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute(DDL)

        cache[db] = conn

    return cache[db]


def read_job_metrics(job_id):
    conn = get_connection(readonly=True)

    raw_metrics = None

    if conn is not None:
        try:
            raw_metrics = conn.execute(
                "SELECT metrics FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise

    if raw_metrics is None:
        metrics = {}
    else:
        metrics = json.loads(raw_metrics["metrics"])
    return defaultdict(float, metrics)


def write_job_metrics(job_id, metrics):
    raw_metrics = json.dumps(metrics)
    get_connection(readonly=False).execute(
        """
        INSERT INTO jobs (id, metrics) VALUES (?, ?)
        ON CONFLICT(id) DO UPDATE set metrics = ?
        """,
        (job_id, raw_metrics, raw_metrics),
    )


def main():
    last_run = None
    while True:
        before = time.time()
        active_jobs = database.find_where(
            models.Job, state__in=[models.State.PENDING, models.State.RUNNING]
        )
        last_run = record_tick_trace(last_run, active_jobs)

        # record_tick_trace might have take a while, so sleep the remainding interval
        # enforce a minimum time of 3s to ensure we don't hammer honeycomb or
        # the docker api
        elapsed = time.time() - before
        time.sleep(max(2, config.STATS_POLL_INTERVAL - elapsed))


def record_tick_trace(last_run, active_jobs):
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
    duration_s = int((end_time - start_time) / 1e9)

    with tracer.start_as_current_span(
        "TICK", start_time=start_time, attributes=trace_attrs
    ) as root:
        # add error event so we can see the error from the docker command
        if error_attrs:
            root.add_event("stats_error", attributes=error_attrs, timestamp=start_time)

        for job in active_jobs:
            # we are using seconds for our metric calculations

            metrics = stats.get(job.id, {})

            # set up attributes
            job_span_attrs = {}
            job_span_attrs.update(trace_attrs)
            job_span_attrs["has_metrics"] = metrics != {}
            job_span_attrs.update(metrics)

            # this means the job is running
            if metrics:
                runtime_s = int(now / 1e9) - job.started_at
                # protect against unexpected runtimes
                if runtime_s > 0:
                    job_metrics = update_job_metrics(
                        job,
                        metrics,
                        duration_s,
                        runtime_s,
                    )
                    job_span_attrs.update(job_metrics)
                else:
                    job_span_attrs.set("bad_tick_runtime", runtime_s)

            # record span
            span = tracer.start_span(job.status_code.name, start_time=start_time)
            tracing.set_span_metadata(span, job, **job_span_attrs)
            span.end(end_time)

    return end_time


def update_job_metrics(job, raw_metrics, duration_s, runtime_s):
    """Update and persist per-job aggregate stats in the metrics db"""

    job_metrics = read_job_metrics(job.id)

    cpu = raw_metrics["cpu_percentage"]
    mem_mb = raw_metrics["memory_used"] / (1024.0 * 1024.0)

    job_metrics["cpu_sample"] = cpu
    job_metrics["cpu_cumsum"] += duration_s * cpu
    job_metrics["cpu_mean"] = job_metrics["cpu_cumsum"] / runtime_s
    job_metrics["cpu_peak"] = max(job_metrics["cpu_peak"], cpu)
    job_metrics["mem_mb_sample"] = mem_mb
    job_metrics["mem_mb_cumsum"] += duration_s * mem_mb
    job_metrics["mem_mb_mean"] = job_metrics["mem_mb_cumsum"] / runtime_s
    job_metrics["mem_mb_peak"] = max(job_metrics["mem_mb_peak"], mem_mb)

    write_job_metrics(job.id, job_metrics)

    return job_metrics


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
