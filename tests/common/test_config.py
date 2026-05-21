import subprocess
import sys
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


_OLD_VERSION_PROJECT_YAML = (
    'version: "1"\n'
    "actions:\n"
    "  my_action:\n"
    "    run: python:v2 script.py\n"
    "    outputs:\n"
    "      moderately_sensitive:\n"
    "        result: output.csv\n"
)


def test_pipeline_old_version_warning_suppressed():
    # Script that defines UserWarnings as errors _before_ importing
    # common.config which contains the "ingore" filter. If our ignore
    # filter regresses, this will cause the ProjectWarning triggered by
    # loading an old version project.yaml raise an error.
    script = f"""
import warnings
warnings.filterwarnings("error", category=UserWarning)
from common import config
from pipeline import load_pipeline
load_pipeline({repr(_OLD_VERSION_PROJECT_YAML)})
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Unexpected warning raised: {result.stderr.strip().splitlines()[-1]}"
    )
