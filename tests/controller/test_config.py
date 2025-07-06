from controller.config import job_limits_from_env
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


def test_job_limits_from_env():
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
