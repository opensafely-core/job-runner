"""
Script runs both jobrunner flows in a single process.
"""

import logging
import threading
import time

from jobrunner import record_stats, sync, tracing
from jobrunner.agent.main import main as agent_main
from jobrunner.config import agent as agent_config
from jobrunner.config import controller as config
from jobrunner.controller.main import main as controller_main
from jobrunner.lib.database import ensure_valid_db
from jobrunner.lib.docker import docker
from jobrunner.lib.log_utils import configure_logging
from jobrunner.queries import get_flag_value, set_flag


log = logging.getLogger(__name__)


def start_thread(func, name, loop_interval):
    """Start a thread running the given function.

    It is wrapped in function that will handle and log any exceptions.  This
    ensures any uncaught exceptions do not leave a zombie thread. We add
    a delay prevents busy retry loops.
    """

    def thread_wrapper():
        while True:
            try:
                func()
            except sync.SyncAPIError as e:
                log.error(e)
            except Exception:
                log.exception(f"Exception in {name} thread")

            time.sleep(loop_interval)

    log.info(f"Starting {name} thread")

    # daemon=True means this thread will be automatically join()ed when the
    # process exits
    thread = threading.Thread(target=thread_wrapper, daemon=True)
    thread.name = name
    thread.start()

    return thread


def main():
    """Run the sync and agent loops in threads, then run the controller loop

    Also runs stats thread and maybe maintenance thread.
    """
    # note: thread name appears in log output, so its nice to keep them all the same length
    threading.current_thread().name = "ctrl"
    fmt = "{asctime} {threadName} {message} {tags}"
    configure_logging(fmt)
    tracing.setup_default_tracing()

    # check db is present and up to date, or else error
    ensure_valid_db()

    try:
        log.info("jobrunner.service started")

        start_thread(sync.main, "sync", config.POLL_INTERVAL * 5)
        start_thread(record_stats.main, "stat", agent_config.STATS_POLL_INTERVAL)
        if config.ENABLE_MAINTENANCE_MODE_THREAD:
            start_thread(maintenance_mode, "mntn", config.MAINTENANCE_POLL_INTERVAL)
        start_thread(agent_main, "agnt", config.POLL_INTERVAL)
        controller_main()
    except KeyboardInterrupt:
        log.info("jobrunner.service stopped")


DB_MAINTENANCE_MODE = "db-maintenance"


def maintenance_mode():
    """Check if the db is currently in maintenance mode, and set flags as appropriate."""
    # TODO currently we get and set flags using the agent's backend ,
    # this will change when the db is only accessible to the controller
    backend = agent_config.BACKEND

    # This did not seem big enough to warrant splitting into a separate module.
    log.info("checking if db undergoing maintenance...")

    # manually setting this flag bypasses the automaticaly check
    manual_db_mode = get_flag_value("manual-db-maintenance", backend)
    if manual_db_mode:
        log.info(f"manually set db mode: {DB_MAINTENANCE_MODE}")
        mode = DB_MAINTENANCE_MODE
    else:
        # detect db mode from TPP.
        current = get_flag_value("mode", backend)
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
            env={"DATABASE_URL": agent_config.DATABASE_URLS["default"]},
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

    set_flag("mode", mode, backend)
    mode = get_flag_value("mode", backend)
    log.info(f"db mode: {mode}")

    # return is only for testing utility
    return mode


if __name__ == "__main__":
    main()
