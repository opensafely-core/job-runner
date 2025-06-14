"""
Script runs all controller flows in a single process.
"""

import logging
import threading

from jobrunner import sync, tracing
from jobrunner.config import agent as agent_config
from jobrunner.config import controller as config
from jobrunner.controller.main import main as controller_main
from jobrunner.controller.ticks import main as ticks_main
from jobrunner.lib.database import ensure_valid_db
from jobrunner.lib.log_utils import configure_logging
from jobrunner.lib.service_utils import ThreadWrapper


log = logging.getLogger(__name__)


start_thread = ThreadWrapper(log)


def main():
    """
    Run the controller loop in the main thread and the sync and tick loops in background
    threads
    """
    # note: thread name appears in log output, so its nice to keep them all the same length
    threading.current_thread().name = "ctrl"
    fmt = "{asctime} {threadName} {message} {tags}"
    configure_logging(fmt)
    tracing.setup_default_tracing()

    # check db is present and up to date, or else error
    ensure_valid_db()

    try:
        log.info("controller.service started")

        start_thread(sync.main, "sync", config.POLL_INTERVAL * 5)
        start_thread(ticks_main, "tick", agent_config.STATS_POLL_INTERVAL)
        controller_main()
    except KeyboardInterrupt:
        log.info("controller.service stopped")


if __name__ == "__main__":
    main()
