from controller.config import client_tokens_from_env, job_limits_from_env
from tests.conftest import import_cfg as import_config_from_script


script = """
from controller import config
cfg = {}
cfg.update({k: str(v) for k, v in vars(config).items() if k.isupper()})
print(repr(cfg))
"""


def import_cfg(env, raises=None):
    return import_config_from_script(env, script, raises=None)


def test_config_imports_with_clean_env():
    import_cfg({})


def test_job_limits_from_env(monkeypatch):
    monkeypatch.setattr("common.config.BACKENDS", ["tpp", "test", "emis"])
    env = {
        "DEFAULT_FOO_LIMIT": "4",
        "TPP_FOO_LIMIT": "10",
        "TEST_FOO_LIMIT": "1",
    }
    assert job_limits_from_env(env, "foo_limit", 6, float) == {
        "tpp": 10.0,
        "test": 1.0,
        "emis": 4.0,
    }


def test_max_workers():
    cfg = import_cfg(
        {"BACKENDS": "foo,bar,test", "FOO_MAX_DB_WORKERS": "3", "BAR_MAX_WORKERS": "7"}
    )
    # overall default is 10 if no default specified in config
    # (test backend is set at 2)
    assert cfg["MAX_WORKERS"] == str({"foo": 10, "bar": 7, "test": 2})

    assert cfg["MAX_DB_WORKERS"] == str(
        {
            "foo": 3,
            "bar": 7,  # default inherited from MAX_WORKERS
            "test": 2,
        }
    )


def test_job_server_tokens():
    cfg = import_cfg({"BACKENDS": "foo,bar", "FOO_JOB_SERVER_TOKEN": "1234"})
    assert cfg["JOB_SERVER_TOKENS"] == str({"foo": "1234", "bar": "token"})


def test_client_tokens():
    # Note this test is similar to the one below, but coverage doesn't recognise
    # that it's exercised all the code due to the use of subprocess in import_cfg
    cfg = import_cfg(
        {
            "BACKENDS": "foo,bar,baz",
            "FOO_CLIENT_TOKENS": "token1,token2",
            "BAR_CLIENT_TOKENS": "token1",
        }
    )
    assert cfg["CLIENT_TOKENS"] == str({"token1": ["foo", "bar"], "token2": ["foo"]})


def test_client_tokens_from_env(monkeypatch):
    monkeypatch.setattr("common.config.BACKENDS", ["foo", "bar", "baz"])
    env = {
        "FOO_CLIENT_TOKENS": "token1,token2",
        "BAR_CLIENT_TOKENS": "token1",
        # A token for an unknown backend is ignored
        "UNKNOWN_CLIENT_TOKENS": "token1",
    }
    assert client_tokens_from_env(env) == {"token1": ["foo", "bar"], "token2": ["foo"]}
