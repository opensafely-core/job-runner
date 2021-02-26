import ast
import subprocess
import sys

script = """
from jobrunner import config;
cfg = {k: str(v) for k, v in vars(config).items() if k.isupper()}
print(repr(cfg))
"""


def import_cfg(env, raises=None):
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
    _, err = import_cfg(
        {
            "PRESTO_TLS_KEY_PATH": "key.notexists",
            "PRESTO_TLS_CERT_PATH": "cert.notexists",
        }
    )
    assert "PRESTO_TLS_KEY_PATH=key.notexists" in err
    assert "PRESTO_TLS_CERT_PATH=cert.notexists" in err

    key = tmp_path / "key"
    key.write_text("key")
    _, err = import_cfg(
        {
            "PRESTO_TLS_KEY_PATH": str(key),
            "PRESTO_TLS_CERT_PATH": "cert.notexists",
        }
    )
    assert "PRESTO_TLS_KEY_PATH=key.notexists" not in err
    assert "PRESTO_TLS_CERT_PATH=cert.notexists" in err

    cert = tmp_path / "cert"
    cert.write_text("cert")
    _, err = import_cfg(
        {
            "PRESTO_TLS_KEY_PATH": "key.notexists",
            "PRESTO_TLS_CERT_PATH": str(cert),
        }
    )
    assert "PRESTO_TLS_KEY_PATH=key.notexists" in err
    assert "PRESTO_TLS_CERT_PATH=cert.notexists" not in err
