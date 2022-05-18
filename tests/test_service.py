import platform
import signal
import subprocess
import sys
import time

import pytest

from jobrunner import config, queries, service


@pytest.mark.skipif(
    platform.system() == "Windows", reason="tricky to do ctrl-c in windows"
)
def test_service_main(tmp_path):
    """
    Test that the service module handles SIGINT and exits cleanly
    """
    p = subprocess.Popen(
        [sys.executable, "-m", "jobrunner.service"],
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


def add_maintenance_command(mock_subprocess_run, current):
    return mock_subprocess_run.add_call(
        [
            "docker",
            "run",
            "--rm",
            "-e",
            "DATABASE_URL",
            "ghcr.io/opensafely-core/cohortextractor",
            "maintenance",
            "--current-mode",
            str(current),
        ],
        env={"DATABASE_URL": config.DATABASE_URLS["full"]},
        capture_output=True,
        text=True,
        check=True,
        timeout=300,
    )


def test_maintenance_mode_off(mock_subprocess_run, db):
    ps = add_maintenance_command(mock_subprocess_run, current=None)
    ps.stdout = ""
    assert service.maintenance_mode() is None

    queries.set_flag("mode", "db-maintenance")
    ps = add_maintenance_command(mock_subprocess_run, current="db-maintenance")
    ps.stdout = ""
    assert service.maintenance_mode() is None


def test_maintenance_mode_on(mock_subprocess_run, db):
    ps = add_maintenance_command(mock_subprocess_run, current=None)
    ps.stdout = "db-maintenance"
    ps.stderr = "other stuff"
    assert service.maintenance_mode() == "db-maintenance"

    queries.set_flag("mode", "db-maintenance")
    ps = add_maintenance_command(mock_subprocess_run, current="db-maintenance")
    ps.stdout = "db-maintenance"
    ps.stderr = "other stuff"
    assert service.maintenance_mode() == "db-maintenance"


def test_maintenance_mode_error(mock_subprocess_run, db):
    ps = add_maintenance_command(mock_subprocess_run, current=None)
    ps.returncode = 1
    ps.stdout = ""
    ps.stderr = "error"

    with pytest.raises(subprocess.CalledProcessError):
        service.maintenance_mode()
