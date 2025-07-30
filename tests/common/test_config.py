from pathlib import Path

from tests.conftest import import_cfg as import_config_from_script


script = """
from common import config
cfg = {}
cfg.update({k: str(v) for k, v in vars(config).items() if k.isupper()})
print(repr(cfg))
"""


def import_cfg(env, raises=None):
    return import_config_from_script(env, script, raises=None)


def test_config_imports_with_clean_env():
    import_cfg({})


def test_version_missing():
    cfg = import_cfg({})
    assert cfg["VERSION"] == "unknown"


def test_version_file():
    cfg = import_cfg(
        {
            "JOBRUNNER_VERSION_FILE_PATH": str(
                Path(__file__).parents[1] / "fixtures/version.txt"
            )
        }
    )
    assert cfg["VERSION"] == "abc1234"
