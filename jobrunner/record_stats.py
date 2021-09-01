"""
Super crude docker/system stats logger
"""
import datetime
import json
import logging
import sqlite3
import sys
import time

from jobrunner import config
from jobrunner.lib.docker_stats import (
    get_container_stats,
    get_volume_and_container_sizes,
)
from jobrunner.lib.log_utils import configure_logging
from jobrunner.lib.system_stats import DockerDiskSpaceError, get_system_stats

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
    while True:
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
    try:
        stats = get_system_stats()
    except DockerDiskSpaceError:
        # Sometimes we're so low on disk space that we can't start the
        # container needed to get system-level stats, in this case it's better
        # to not get the stats than to bail entirely (and spam the logs in the
        # process)
        stats = {}
    volume_sizes, container_sizes = get_volume_and_container_sizes()
    containers = get_container_stats()
    for name, container in containers.items():
        container["disk_used"] = container_sizes.get(name)
    stats["containers"] = containers
    stats["volumes"] = volume_sizes
    return stats


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
