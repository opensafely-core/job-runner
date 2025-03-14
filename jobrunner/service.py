"""
Script runs both jobrunner flows in a single process.
"""

import logging
import threading
import time

from jobrunner import config, record_stats, run, sync, tracing
from jobrunner.lib.database import ensure_valid_db
from jobrunner.lib.docker import docker
from jobrunner.lib.log_utils import configure_logging
from jobrunner.queries import get_flag_value, set_flag


log = logging.getLogger(__name__)


def start_thread(target, name):
    log.info(f"Starting {name} thread")
    # daemon=True means this thread will be automatically join()ed when the
    # process exits
    thread = threading.Thread(target=target, daemon=True)
    thread.name = name
    thread.start()


def main():
    """Run the main run loop after starting the sync loop in a thread."""
    # extra space to align with other thread's "sync" label.
    threading.current_thread().name = "run "
    fmt = "{asctime} {threadName} {message} {tags}"
    configure_logging(fmt)
    tracing.setup_default_tracing()

    # check db is present and up to date, or else error
    ensure_valid_db()

    try:
        log.info("jobrunner.service started")
        # note: thread name appears in log output, so its nice to keep them all the same length
        start_thread(sync_wrapper, "sync")
        start_thread(record_stats_wrapper, "stat")
        if config.ENABLE_MAINTENANCE_MODE_THREAD:
            start_thread(maintenance_wrapper, "mntn")
        run.main()
    except KeyboardInterrupt:
        log.info("jobrunner.service stopped")


def sync_wrapper():
    """Wrap the sync call with logging context and an exception handler."""
    # avoid busy retries on hard failure
    sleep_after_error = config.POLL_INTERVAL * 5
    while True:
        try:
            sync.main()
        except sync.SyncAPIError as e:
            # Handle these separately as we don't want the full traceback here,
            # just the text of the error response
            log.error(e)
            time.sleep(sleep_after_error)
        except Exception:
            log.exception("Exception in sync thread")
            time.sleep(sleep_after_error)


def record_stats_wrapper():
    """Wrap the record_stats call with logging context and an exception handler."""
    while True:
        try:
            record_stats.main()
            # `main()` should loop forever, if it exits cleanly that means it
            # wasn't configured to run so we should exit the thread rather than
            # looping
            return
        except Exception:
            log.exception("Exception in record_stats thread")
            time.sleep(config.STATS_POLL_INTERVAL)


def maintenance_wrapper():
    """Poll a backend specific way to set the db maintenance flag."""
    while True:
        try:
            maintenance_mode()
        except Exception:
            log.exception("Exception in maintenance_mode thread")

        time.sleep(config.MAINTENANCE_POLL_INTERVAL)


DB_MAINTENANCE_MODE = "db-maintenance"


def maintenance_mode():
    """Check if the db is currently in maintenance mode, and set flags as appropriate."""
    # This did not seem big enough to warrant splitting into a separate module.
    log.info("checking if db undergoing maintenance...")

    # manually setting this flag bypasses the automaticaly check
    manual_db_mode = get_flag_value("manual-db-maintenance")
    if manual_db_mode:
        log.info(f"manually set db mode: {DB_MAINTENANCE_MODE}")
        mode = DB_MAINTENANCE_MODE
    else:

        # detect db mode from TPP.
        current = get_flag_value("mode")
        ps = docker(
            [
                "run",
                "--rm",
                "-e",
                "DATABASE_URL",
                "ghcr.io/opensafely-core/cohortextractor",
                "maintenance",
                "--current-mode",
                str(current),
            ],
            env={"DATABASE_URL": config.DATABASE_URLS["default"]},
            check=True,
            capture_output=True,
            text=True,
        )
        last_line = ps.stdout.strip().split("\n")[-1]

        if DB_MAINTENANCE_MODE in last_line:
            if current != DB_MAINTENANCE_MODE:
                log.warning("Enabling DB maintenance mode")
            else:
                log.warning("DB maintenance mode is currently enabled")

            mode = DB_MAINTENANCE_MODE
        else:
            if current == DB_MAINTENANCE_MODE:
                log.info("DB maintenance mode had ended")
            mode = None

    set_flag("mode", mode)
    mode = get_flag_value("mode")
    log.info(f"db mode: {mode}")
    return mode


if __name__ == "__main__":
    main()
