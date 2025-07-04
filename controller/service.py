"""
Script runs all controller flows in a single process.
"""

import logging
import threading

from common.lib.log_utils import configure_logging
from common.lib.service_utils import ThreadWrapper
from controller import config, sync
from controller.lib.database import ensure_valid_db
from controller.main import main as controller_main
from controller.ticks import main as ticks_main
from jobrunner import tracing


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
        start_thread(ticks_main, "tick", config.TICK_POLL_INTERVAL)
        controller_main()
    except KeyboardInterrupt:
        log.info("controller.service stopped")


if __name__ == "__main__":
    main()
