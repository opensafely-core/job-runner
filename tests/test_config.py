import ast
import subprocess
import sys

import pytest

from jobrunner.config.agent import _is_valid_backend_name, database_urls_from_env


script = """
from jobrunner.config import agent, common, controller
cfg = {}
for module in [agent, common, controller]:
    cfg.update({k: str(v) for k, v in vars(module).items() if k.isupper()})
print(repr(cfg))
"""


def import_cfg(env, raises=None):
    try:
        ps = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            text=True,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as err:
        print(err.stderr)
        raise

    print(ps.stdout)
    return ast.literal_eval(ps.stdout)


def test_config_imports_with_clean_env():
    import_cfg({})


def test_config_presto_paths(tmp_path):
    key = tmp_path / "key"
    key.write_text("key")
    cert = tmp_path / "cert"
    cert.write_text("cert")
    print(key)
    print(cert)
    cfg = import_cfg(
        {"PRESTO_TLS_KEY_PATH": str(key), "PRESTO_TLS_CERT_PATH": str(cert)}
    )
    assert cfg["PRESTO_TLS_KEY"] == "key"
    assert cfg["PRESTO_TLS_CERT"] == "cert"


def test_config_presto_paths_not_exist(tmp_path):
    key = tmp_path / "key"
    key.write_text("key")
    cert = tmp_path / "cert"
    cert.write_text("cert")

    cfg = import_cfg(
        {
            "PRESTO_TLS_KEY_PATH": str(key),
            "PRESTO_TLS_CERT_PATH": str(cert),
        }
    )
    assert cfg["PRESTO_TLS_KEY"] == "key"
    assert cfg["PRESTO_TLS_CERT"] == "cert"

    with pytest.raises(subprocess.CalledProcessError) as err:
        import_cfg({"PRESTO_TLS_KEY_PATH": "foo"})

    assert "Both PRESTO_TLS_KEY_PATH and PRESTO_TLS_CERT_PATH must be defined" in str(
        err.value.stderr
    )

    with pytest.raises(subprocess.CalledProcessError) as err:
        import_cfg(
            {
                "PRESTO_TLS_KEY_PATH": "key.notexists",
                "PRESTO_TLS_CERT_PATH": str(cert),
            }
        )

    assert "PRESTO_TLS_KEY_PATH=key.notexists" in str(err.value.stderr)

    with pytest.raises(subprocess.CalledProcessError) as err:
        import_cfg(
            {
                "PRESTO_TLS_KEY_PATH": str(key),
                "PRESTO_TLS_CERT_PATH": "cert.notexists",
            }
        )

    assert "PRESTO_TLS_CERT_PATH=cert.notexists" in str(err.value.stderr)


@pytest.mark.parametrize(
    "env_value,expected",
    [
        ("Yes", True),
        ("true", True),
        ("1", True),
        ("false", False),
        ("foo", False),
    ],
)
def test_config_truthy(env_value, expected):
    cfg = import_cfg({"USING_DUMMY_DATA_BACKEND": env_value})
    assert cfg["USING_DUMMY_DATA_BACKEND"] == str(expected)


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


def test_database_urls_from_env():
    db_urls = database_urls_from_env(
        {
            "DEFAULT_DATABASE_URL": "mssql://localhost/db1",
            "INCLUDE_T1OO_DATABASE_URL": "mssql://localhost/db2",
        }
    )
    assert db_urls == {
        "default": "mssql://localhost/db1",
        "include_t1oo": "mssql://localhost/db2",
    }
