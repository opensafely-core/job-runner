"""
Script runs both jobrunner flows in a single process.
"""
import logging
import time
import threading

from . import config
from .log_utils import configure_logging
from . import run
from . import sync


log = logging.getLogger(__name__)


def main():
    """Run the main run loop after starting the sync loop in a thread."""
    # extra space to align with other thread's "sync" label.
    threading.current_thread().name = "run "
    fmt = "{asctime} {threadName} {message} {tags}"
    configure_logging(fmt)

    try:
        log.info("jobrunner.service started")
        # daemon=True means this thread will be automatically join()ed when the
        # process exits
        thread = threading.Thread(target=sync_wrapper, daemon=True)
        thread.name = "sync"
        thread.start()
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


if __name__ == "__main__":
    main()
