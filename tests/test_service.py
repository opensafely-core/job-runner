import platform
import signal
import subprocess
import sys
import time

import pytest

from jobrunner import config, queries, service
from jobrunner.lib import database


@pytest.mark.skipif(
    platform.system() == "Windows", reason="tricky to do ctrl-c in windows"
)
def test_service_main(tmp_path):
    """
    Test that the service module handles SIGINT and exits cleanly
    """
    db = tmp_path / "db.sqlite"
    database.ensure_db(db)

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
        env={"DATABASE_URL": config.DATABASE_URLS["default"]},
        capture_output=True,
        text=True,
        check=True,
        timeout=300,
    )


def test_maintenance_mode_off(mock_subprocess_run, db, db_config):
    ps = add_maintenance_command(mock_subprocess_run, current=None)
    ps.stdout = ""
    assert service.maintenance_mode() is None
    assert queries.get_flag("mode").value is None

    queries.set_flag("mode", "db-maintenance")
    ps = add_maintenance_command(mock_subprocess_run, current="db-maintenance")
    ps.stdout = ""
    assert service.maintenance_mode() is None
    assert queries.get_flag("mode").value is None


def test_maintenance_mode_on(mock_subprocess_run, db, db_config):
    ps = add_maintenance_command(mock_subprocess_run, current=None)
    ps.stdout = "db-maintenance"
    ps.stderr = "other stuff"
    assert service.maintenance_mode() == "db-maintenance"
    assert queries.get_flag("mode").value == "db-maintenance"

    queries.set_flag("mode", "db-maintenance")
    ps = add_maintenance_command(mock_subprocess_run, current="db-maintenance")
    ps.stdout = "db-maintenance"
    ps.stderr = "other stuff"
    assert service.maintenance_mode() == "db-maintenance"
    assert queries.get_flag("mode").value == "db-maintenance"


def test_maintenance_mode_error(mock_subprocess_run, db, db_config):
    ps = add_maintenance_command(mock_subprocess_run, current=None)
    ps.returncode = 1
    ps.stdout = ""
    ps.stderr = "error"

    with pytest.raises(subprocess.CalledProcessError):
        service.maintenance_mode()


def test_maintenance_mode_manual(db, db_config):
    queries.set_flag("manual-db-maintenance", "on")
    assert service.maintenance_mode() == "db-maintenance"
    assert queries.get_flag("mode").value == "db-maintenance"


@pytest.fixture
def db_config(monkeypatch):
    monkeypatch.setitem(config.DATABASE_URLS, "default", "mssql://localhost")
