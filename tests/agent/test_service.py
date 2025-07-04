import signal
import subprocess
import sys
import time


def test_service_main(tmp_path):
    """
    Test that the service module handles SIGINT and exits cleanly
    """
    p = subprocess.Popen(
        [sys.executable, "-m", "agent.service"],
        # For the purposes of this test we don't care if we can actually talk to the
        # task endpoint, so we just point it to a "reserved for future use" IP4 block
        # which hangs nicely as we want
        env={
            "WORKDIR": str(tmp_path),
            "CONTROLLER_TASK_API_ENDPOINT": "https://240.0.0.1/",
        },
    )
    assert p.returncode is None
    time.sleep(3)
    p.send_signal(signal.SIGINT)
    p.wait()
    assert p.returncode == 0
