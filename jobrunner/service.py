"""
Script runs both jobrunner flows in a single process.
"""
import datetime
import logging
from pathlib import Path
import os
import sys
import time
import threading

log = logging.getLogger(__name__)

# temp workaround for not having PYTHONPATH in the service config
sys.path.insert(0, "./lib")

# load any env file *before* we import config module
# TODO: we should probably have a wrapper that does this before python is
# loaded, or make config reloadable.
def parse_env(contents):
    """Parse a simple environment file."""
    env = {}
    for line in contents.split("\n"):
        line = line.strip()
        if not line or line[0] == "#":
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


path = Path(os.environ.get("ENVPATH", ".env"))
if path.exists():
    log.info(f"Loading environment variables from {path}")
    env = parse_env(path.read_text())
    if env:
        os.environ.update(env)

from . import config
from .log_utils import configure_logging
from . import run
from . import sync


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
    while True:
        try:
            sync.main()
        except Exception:
            log.exception("Exception in sync thread")
            # avoid busy retries on hard failure
            time.sleep(config.POLL_INTERVAL)


if __name__ == "__main__":
    main()
