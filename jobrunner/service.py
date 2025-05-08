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
from jobrunner.lib.log_utils import configure_logging


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
        start_thread(agent_main, "agnt", config.POLL_INTERVAL)
        controller_main()
    except KeyboardInterrupt:
        log.info("jobrunner.service stopped")


if __name__ == "__main__":
    main()
