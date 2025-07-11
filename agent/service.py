"""
Script runs all agent flows in a single process.
"""

import logging
import threading

from agent import config
from agent.main import main as agent_main
from agent.metrics import main as metrics_main
from common import tracing
from common.lib.log_utils import configure_logging
from common.lib.service_utils import ThreadWrapper


log = logging.getLogger(__name__)


start_thread = ThreadWrapper(log)


def main():
    """
    Run the agent loop in the main thread and the metrics loop in a background thread
    """
    # note: thread name appears in log output, so its nice to keep them all the same length
    threading.current_thread().name = "agnt"
    fmt = "{asctime} {threadName} {message} {tags}"
    configure_logging(fmt)
    tracing.setup_default_tracing("agent")

    try:
        log.info("agent.service started")

        start_thread(metrics_main, "mtrc", config.STATS_POLL_INTERVAL)
        agent_main()
    except KeyboardInterrupt:
        log.info("agent.service stopped")


if __name__ == "__main__":
    main()
