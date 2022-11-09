"""
Super crude docker/system stats logger
"""
import datetime
import json
import logging
import sqlite3
import sys
import time

from opentelemetry import trace

from jobrunner import config, models, tracing
from jobrunner.lib import database
from jobrunner.lib.docker_stats import (
    get_container_stats,
    get_volume_and_container_sizes,
)
from jobrunner.lib.log_utils import configure_logging


SCHEMA_SQL = """
CREATE TABLE stats (
    timestamp TEXT,
    data TEXT
);
"""


log = logging.getLogger(__name__)


def main():
    database_file = config.STATS_DATABASE_FILE
    if not database_file:
        log.info("STATS_DATABASE_FILE not set; not polling for system stats")
        return
    log.info(f"Logging system stats to: {database_file}")
    connection = get_database_connection(database_file)
    last_run = None
    while True:
        last_run = record_tick_trace(last_run)
        log_stats(connection)
        time.sleep(config.STATS_POLL_INTERVAL)


def get_database_connection(filename):
    filename.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(filename)
    # Enable autocommit
    conn.isolation_level = None
    schema_count = list(conn.execute("SELECT COUNT(*) FROM sqlite_master"))[0][0]
    if schema_count == 0:
        conn.executescript(SCHEMA_SQL)
    return conn


def log_stats(connection):
    stats = get_all_stats()
    # If no containers are running then don't log anything
    if not stats["containers"]:
        return
    timestamp = datetime.datetime.utcnow().isoformat()
    connection.execute(
        "INSERT INTO stats (timestamp, data) VALUES (?, ?)",
        [timestamp, json.dumps(stats)],
    )


def get_all_stats():
    volume_sizes, container_sizes = get_volume_and_container_sizes()
    containers = get_container_stats()
    for name, container in containers.items():
        container["disk_used"] = container_sizes.get(name)
    return {
        "containers": containers,
        "volumes": volume_sizes,
    }


tracer = trace.get_tracer("ticks")


def record_tick_trace(last_run):
    """Record a period tick trace of current jobs.

    This will give us more realtime information than the job traces, which only
    send spans data when *leaving* a state.

    The easiest way to filter these in honeycomb is on tick==true attribute

    Not that this will emit number of active jobs + 1 events every call, so we
    don't want to call it on too tight a loop.
    """
    now = time.time_ns()

    if last_run is None:
        return now

    # every span has the same timings
    start_time = last_run
    end_time = now

    active_jobs = database.find_where(
        models.Job, state__in=[models.State.PENDING, models.State.RUNNING]
    )

    with tracer.start_as_current_span("TICK", start_time=start_time):
        for job in active_jobs:
            span = tracer.start_span(job.status_code.name, start_time=start_time)
            # TODO add cpu/memory as attributes?
            tracing.set_span_metadata(span, job, tick=True)
            span.end(end_time)

    return end_time


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
