import ast
import os
import subprocess
import sys

import pytest

from jobrunner.config import _is_valid_backend_name


script = """
from jobrunner import config;
cfg = {k: str(v) for k, v in vars(config).items() if k.isupper()}
print(repr(cfg))
"""


def import_cfg(env, raises=None):
    # Required for Python to start correctly on Windows, otherwise we get:
    #
    #   Fatal Python error: _Py_HashRandomization_Init: failed to get random
    #   numbers to initialize Python
    #
    # See https://stackoverflow.com/a/64706392
    if "SYSTEMROOT" in os.environ:
        env["SYSTEMROOT"] = os.environ["SYSTEMROOT"]

    ps = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        text=True,
        capture_output=True,
    )
    if ps.returncode == 0:
        print(ps.stdout)
        return ast.literal_eval(ps.stdout), None
    else:
        return None, ps.stderr


def test_config_imports_with_clean_env():
    import_cfg({})


def test_config_presto_paths(tmp_path):
    key = tmp_path / "key"
    key.write_text("key")
    cert = tmp_path / "cert"
    cert.write_text("cert")
    cfg, err = import_cfg(
        {"PRESTO_TLS_KEY_PATH": str(key), "PRESTO_TLS_CERT_PATH": str(cert)}
    )
    assert err is None
    assert cfg["PRESTO_TLS_KEY"] == "key"
    assert cfg["PRESTO_TLS_CERT"] == "cert"


def test_config_presto_paths_not_exist(tmp_path):

    key = tmp_path / "key"
    key.write_text("key")
    cert = tmp_path / "cert"
    cert.write_text("cert")

    cfg, err = import_cfg(
        {
            "PRESTO_TLS_KEY_PATH": str(key),
            "PRESTO_TLS_CERT_PATH": str(cert),
        }
    )
    assert cfg["PRESTO_TLS_KEY"] == "key"
    assert cfg["PRESTO_TLS_CERT"] == "cert"

    # only one
    _, err = import_cfg({"PRESTO_TLS_KEY_PATH": "foo"})
    assert "Both PRESTO_TLS_KEY_PATH and PRESTO_TLS_CERT_PATH must be defined" in err

    cfg, err = import_cfg(
        {
            "PRESTO_TLS_KEY_PATH": "key.notexists",
            "PRESTO_TLS_CERT_PATH": str(cert),
        }
    )
    assert "PRESTO_TLS_KEY_PATH=key.notexists" in err

    cfg, err = import_cfg(
        {
            "PRESTO_TLS_KEY_PATH": str(key),
            "PRESTO_TLS_CERT_PATH": "cert.notexists",
        }
    )
    assert "PRESTO_TLS_CERT_PATH=cert.notexists" in err


@pytest.mark.parametrize(
    "name,is_valid",
    [
        ("foo_BAR-1", True),
        ("foo_BAR-", False),
        (" foo", False),
        ("foo@bar", False),
    ],
)
def test_is_valid_backend_name(name, is_valid):
    assert _is_valid_backend_name(name) == is_valid
