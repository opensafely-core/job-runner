import configparser
import os
import re
from multiprocessing import cpu_count
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


# TODO Add a BACKENDS config and validate each job is from a known backend
# We'll also use the BACKENDS config for looping through BACKENDS in sync
BACKENDS = []

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

# TODO per-backend
MAX_WORKERS = int(os.environ.get("MAX_WORKERS") or max(cpu_count() - 1, 1))
# TODO per-backend
MAX_DB_WORKERS = int(os.environ.get("MAX_DB_WORKERS") or MAX_WORKERS)
# TODO per-backend
LEVEL4_MAX_FILESIZE = int(
    os.environ.get("LEVEL4_MAX_FILESIZE", 16 * 1024 * 1024)
)  # 16mb
# TODO per-backend
LEVEL4_MAX_CSV_ROWS = int(os.environ.get("LEVEL4_MAX_CSV_ROWS", 5000))
# TODO per-backend
LEVEL4_FILE_TYPES = pipeline.constants.LEVEL4_FILE_TYPES

STATA_LICENSE = os.environ.get("STATA_LICENSE")


ACTIONS_GITHUB_ORG = "opensafely-actions"
ACTIONS_GITHUB_ORG_URL = f"https://github.com/{ACTIONS_GITHUB_ORG}"

ALLOWED_GITHUB_ORGS = (
    os.environ.get("ALLOWED_GITHUB_ORGS", "opensafely").strip().split(",")
)


# TODO per-backend
def parse_job_resource_weights(config_file):
    """
    Parse a simple ini file which looks like this:

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
    config_file = Path(config_file)
    weights = {}
    if config_file.exists():
        config = configparser.ConfigParser()
        config.read_string(config_file.read_text(), source=str(config_file))
        for workspace in config.sections():
            weights[workspace] = {
                re.compile(pattern): float(weight)
                for (pattern, weight) in config.items(workspace)
            }
    return weights


JOB_RESOURCE_WEIGHTS = parse_job_resource_weights("job-resource-weights.ini")

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

# TODO per-backend
DEFAULT_JOB_CPU_COUNT = float(os.environ.get("DEFAULT_JOB_CPU_COUNT", 2))
# TODO per-backend
DEFAULT_JOB_MEMORY_LIMIT = os.environ.get("DEFAULT_JOB_MEMORY_LIMIT", "4G")
