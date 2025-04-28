import configparser
import os
import re
from pathlib import Path

import pipeline

from jobrunner.config import common


class ConfigException(Exception):
    pass


DATABASE_FILE = common.WORKDIR / "db.sqlite"

JOB_SERVER_ENDPOINT = os.environ.get(
    "JOB_SERVER_ENDPOINT", "https://jobs.opensafely.org/api/v2/"
)
JOB_SERVER_TOKEN = os.environ.get("JOB_SERVER_TOKEN", "token")

POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "5"))

ALLOWED_IMAGES = {
    "cohortextractor",
    "databuilder",
    "ehrql",
    "stata-mp",
    "r",
    "jupyter",
    "python",
    "sqlrunner",
}

#  Set workers per-backend. This will be used by the controller to
# determine if there are enough resources available to start a new
# job running.
MAX_WORKERS = {
    "test": int(os.environ.get("TEST_MAX_WORKERS") or 3),
    "tpp": int(os.environ.get("TPP_MAX_WORKERS") or 10),
    "emis": int(os.environ.get("EMIS_MAX_WORKERS") or 10),
}
MAX_DB_WORKERS = {
    "test": int(os.environ.get("TEST_MAX_DB_WORKERS") or MAX_WORKERS["test"]),
    "tpp": int(os.environ.get("TPP_MAX_DB_WORKERS") or MAX_WORKERS["tpp"]),
    "emis": int(os.environ.get("EMIS_MAX_DB_WORKERS") or MAX_WORKERS["emis"]),
}

# Currently we assume all backends will have the same
# limits on L4 files
LEVEL4_MAX_FILESIZE = int(
    os.environ.get("LEVEL4_MAX_FILESIZE", 16 * 1024 * 1024)
)  # 16mb
LEVEL4_MAX_CSV_ROWS = int(os.environ.get("LEVEL4_MAX_CSV_ROWS", 5000))
LEVEL4_FILE_TYPES = pipeline.constants.LEVEL4_FILE_TYPES

STATA_LICENSE = os.environ.get("STATA_LICENSE")

ACTIONS_GITHUB_ORG = "opensafely-actions"
ACTIONS_GITHUB_ORG_URL = f"https://github.com/{ACTIONS_GITHUB_ORG}"

ALLOWED_GITHUB_ORGS = (
    os.environ.get("ALLOWED_GITHUB_ORGS", "opensafely").strip().split(",")
)


def parse_job_resource_weights(config_file_template):
    """
    Parse a simple ini file per backend which looks like this:

        [some-workspace-name]
        my-ram-hungry-action = 4
        other-actions.* = 1.5

        [other-workspace-name]
        ...

    Any jobs in the specified workspace will have their action names matched
    against the regex patterns specified in the config file and will be
    assigned the weight of the first matching pattern. All other jobs are
    assigned a weight of 1.
    """
    weights = {}
    for backend in common.BACKENDS:
        weights[backend] = {}
        config_file = common.WORKDIR / Path(
            config_file_template.format(backend=backend.lower())
        )
        if config_file.exists():
            config = configparser.ConfigParser()
            config.read_string(config_file.read_text(), source=str(config_file))
            for workspace in config.sections():
                weights[backend][workspace] = {
                    re.compile(pattern): float(weight)
                    for (pattern, weight) in config.items(workspace)
                }
    return weights


JOB_RESOURCE_WEIGHTS = parse_job_resource_weights("job-resource-weights_{backend}.ini")

MAINTENANCE_POLL_INTERVAL = float(
    os.environ.get("MAINTENANCE_POLL_INTERVAL", "300")
)  # 5 min
# TODO: will be replaced by Task
ENABLE_MAINTENANCE_MODE_THREAD = os.environ.get(
    "ENABLE_MAINTENANCE_MODE_THREAD", ""
).lower() in (
    "true",
    "yes",
    "on",
)

# Map known exit codes to user-friendly messages
DATABASE_EXIT_CODES = {
    # Custom database-related exit codes return from cohortextractor, see
    # https://github.com/opensafely-core/cohort-extractor/blob/0a314a909817dbcc48907643e0b6eeff319337db/cohortextractor/cohortextractor.py#L787
    3: (
        "A transient database error occurred, your job may run "
        "if you try it again, if it keeps failing then contact tech support"
    ),
    4: "New data is being imported into the database, please try again in a few hours",
    5: "Something went wrong with the database, please contact tech support",
}


# per-backend job limits
def job_limits_from_env(env, limit_name, default, transform=str):
    common_default = transform(env.get(f"DEFAULT_{limit_name.upper()}") or default)
    return {
        backend: transform(
            env.get(f"{backend.upper()}_{limit_name.upper()}") or common_default
        )
        for backend in common.BACKENDS
    }


DEFAULT_JOB_CPU_COUNT = job_limits_from_env(os.environ, "job_cpu_count", 2, float)
DEFAULT_JOB_MEMORY_LIMIT = job_limits_from_env(os.environ, "job_memory_limit", "4G")
