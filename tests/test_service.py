from textwrap import dedent
import signal
import subprocess
import sys
import time

from jobrunner import service
from jobrunner.subprocess_utils import subprocess_run


def test_service_main():
    p = subprocess.Popen([sys.executable, "-m", "jobrunner.service"])
    assert p.returncode is None
    time.sleep(3)
    p.send_signal(signal.SIGINT)
    p.wait()
    assert p.returncode == 0


def test_parse_env():
    env = service.parse_env(
        dedent(
            """\
        key=value
        spaces_value=val ue
        spaces key = value
           whitespace\t  =  value  
        single='val ue'
        double="val ue"
    """
        )
    )
    assert env == {
        "key": "value",
        "spaces_value": "val ue",
        "spaces key": "value",
        "whitespace": "value",
        "single": "val ue",
        "double": "val ue",
    }
