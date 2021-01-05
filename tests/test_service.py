from textwrap import dedent
import platform
import signal
import subprocess
import sys
import time

import pytest

from jobrunner import service
from jobrunner.subprocess_utils import subprocess_run


@pytest.mark.skipif(
    platform.system() == "Windows", reason="tricky to do ctrl-c in windows"
)
def test_service_main():
    p = subprocess.Popen([sys.executable, "-m", "jobrunner.service"])
    assert p.returncode is None
    time.sleep(3)
    p.send_signal(signal.SIGINT)
    p.wait()
    assert p.returncode == 0
