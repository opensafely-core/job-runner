from textwrap import dedent
import logging
from pathlib import Path

import jobrunner


def test_parse_env():
    env = jobrunner.parse_env(
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


def test_load_env(tmp_path):
    logs = []
    jobrunner.load_env(Path("doesnotexist"), logs)
    assert logs == [(logging.WARNING, "Could not find environment file doesnotexist")]

    logs = []
    empty = tmp_path / "empty"
    empty.write_text("")
    jobrunner.load_env(empty, logs)
    assert logs == [
        (logging.WARNING, f"Could not parse environment variables from {empty}")
    ]
