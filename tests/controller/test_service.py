import signal
import subprocess
import sys
import threading
import time
from unittest.mock import patch

import pytest

from controller import service
from controller.lib import database


@pytest.fixture(autouse=True)
def restore_main_thread_name():
    """Restore main thread name after each test to avoid leaking global state."""
    original_thread_name = threading.current_thread().name
    yield
    threading.current_thread().name = original_thread_name


def test_service_starts_ticks_when_enabled(db, monkeypatch):
    monkeypatch.setattr("controller.config.CONTROLLER_ENABLE_TICKS", True)
    monkeypatch.setattr("controller.config.TICK_POLL_INTERVAL", 123)

    with patch("controller.service.start_thread", autospec=True) as mock_start_thread:
        with patch(
            "controller.service.controller_main",
            autospec=True,
            side_effect=KeyboardInterrupt,
        ):
            service.main()

    mock_start_thread.assert_called_once_with(service.ticks_main, "tick", 123)


def test_service_does_not_start_ticks_when_disabled(db, monkeypatch):
    monkeypatch.setattr("controller.config.CONTROLLER_ENABLE_TICKS", False)

    with patch("controller.service.start_thread", autospec=True) as mock_start_thread:
        with patch(
            "controller.service.controller_main",
            autospec=True,
            side_effect=KeyboardInterrupt,
        ):
            service.main()

    mock_start_thread.assert_not_called()


def test_service_main(tmp_path):
    """
    Test that the service module handles SIGINT and exits cleanly
    """
    db = tmp_path / "db.sqlite"
    database.ensure_db(db)

    p = subprocess.Popen(
        [sys.executable, "-m", "controller.service"],
        env={
            "WORKDIR": str(tmp_path),
        },
    )
    assert p.returncode is None
    time.sleep(3)
    p.send_signal(signal.SIGINT)
    p.wait()
    assert p.returncode == 0
