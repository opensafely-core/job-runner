import signal
import subprocess
import sys
import time

from jobrunner.lib import database


def test_service_main(tmp_path):
    """
    Test that the service module handles SIGINT and exits cleanly
    """
    db = tmp_path / "db.sqlite"
    database.ensure_db(db)

    p = subprocess.Popen(
        [sys.executable, "-m", "jobrunner.controller.service"],
        # For the purposes of this test we don't care if we can actually talk
        # to the job-server endpoint, so to avoid spamming the real job-server
        # we just point it to a "reserved for future use" IP4 block which hangs
        # nicely as we want.
        env={
            "WORKDIR": str(tmp_path),
            "JOB_SERVER_ENDPOINT": "https://240.0.0.1",
        },
    )
    assert p.returncode is None
    time.sleep(3)
    p.send_signal(signal.SIGINT)
    p.wait()
    assert p.returncode == 0
