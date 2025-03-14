import configparser
import os
import re
import subprocess
import sys
from multiprocessing import cpu_count
from pathlib import Path

import pipeline


class ConfigException(Exception):
    pass


def _is_valid_backend_name(name):
    return bool(re.match(r"^[A-Za-z0-9][A-Za-z0-9_\-]*[A-Za-z0-9]$", name))


default_work_dir = Path(__file__) / "../../workdir"


VERSION = os.environ.get("VERSION", "unknown")
if VERSION == "unknown":
    try:
        ps = subprocess.run(
            ["git", "describe", "--tags"],
            text=True,
            capture_output=True,
        )
        VERSION = ps.stdout.strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass


GIT_SHA = os.environ.get("GIT_SHA", "unknown")
if GIT_SHA == "unknown":
    try:
        ps = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            text=True,
            capture_output=True,
        )
        GIT_SHA = ps.stdout.strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass


WORKDIR = Path(os.environ.get("WORKDIR", default_work_dir)).resolve()
DATABASE_FILE = WORKDIR / "db.sqlite"
METRICS_FILE = WORKDIR / "metrics.sqlite"
GIT_REPO_DIR = WORKDIR / "repos"

# valid archive formats
ARCHIVE_FORMATS = (".tar.gz", ".tar.zstd", ".tar.xz")


JOB_SERVER_ENDPOINT = os.environ.get(
    "JOB_SERVER_ENDPOINT", "https://jobs.opensafely.org/api/v2/"
)
JOB_SERVER_TOKEN = os.environ.get("JOB_SERVER_TOKEN", "token")

PRIVATE_REPO_ACCESS_TOKEN = os.environ.get("PRIVATE_REPO_ACCESS_TOKEN", "")

POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "5"))
JOB_LOOP_INTERVAL = float(os.environ.get("JOB_LOOP_INTERVAL", "1.0"))

BACKEND = os.environ.get("BACKEND", "expectations")
if not _is_valid_backend_name(BACKEND):
    raise RuntimeError(f"BACKEND not in valid format: '{BACKEND}'")

truthy = ("true", "1", "yes")

if os.environ.get("USING_DUMMY_DATA_BACKEND", "false").lower().strip() in truthy:
    USING_DUMMY_DATA_BACKEND = True
else:
    USING_DUMMY_DATA_BACKEND = BACKEND == "expectations"

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

DOCKER_REGISTRY = os.environ.get("DOCKER_REGISTRY", "ghcr.io/opensafely-core")


def database_urls_from_env(env):
    db_names = ["default", "include_t1oo"]
    return {
        db_name: db_url
        for db_name, db_url in [
            (db_name, env.get(f"{db_name.upper()}_DATABASE_URL"))
            for db_name in db_names
        ]
        if db_url
    }


DATABASE_URLS = database_urls_from_env(os.environ)


TEMP_DATABASE_NAME = os.environ.get("TEMP_DATABASE_NAME")

EMIS_ORGANISATION_HASH = os.environ.get("EMIS_ORGANISATION_HASH")
PRESTO_TLS_KEY = PRESTO_TLS_CERT = None
PRESTO_TLS_CERT_PATH = os.environ.get("PRESTO_TLS_CERT_PATH")
PRESTO_TLS_KEY_PATH = os.environ.get("PRESTO_TLS_KEY_PATH")

if bool(PRESTO_TLS_KEY_PATH) != bool(PRESTO_TLS_CERT_PATH):
    raise ConfigException(
        "Both PRESTO_TLS_KEY_PATH and PRESTO_TLS_CERT_PATH must be defined if either are"
    )

if PRESTO_TLS_KEY_PATH:
    key_path = Path(PRESTO_TLS_KEY_PATH)
    if key_path.exists():
        PRESTO_TLS_KEY = key_path.read_text()
    else:
        raise ConfigException(
            f"PRESTO_TLS_KEY_PATH={key_path}, but file does not exist"
        )

if PRESTO_TLS_CERT_PATH:
    cert_path = Path(PRESTO_TLS_CERT_PATH)
    if cert_path.exists():
        PRESTO_TLS_CERT = cert_path.read_text()
    else:
        raise ConfigException(
            f"PRESTO_TLS_CERT_PATH={cert_path}, but file does not exist"
        )


MAX_WORKERS = int(os.environ.get("MAX_WORKERS") or max(cpu_count() - 1, 1))
MAX_DB_WORKERS = int(os.environ.get("MAX_DB_WORKERS") or MAX_WORKERS)
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 0))


LEVEL4_MAX_FILESIZE = int(
    os.environ.get("LEVEL4_MAX_FILESIZE", 16 * 1024 * 1024)
)  # 16mb

LEVEL4_MAX_CSV_ROWS = int(os.environ.get("LEVEL4_MAX_CSV_ROWS", 5000))

LEVEL4_FILE_TYPES = pipeline.constants.LEVEL4_FILE_TYPES

STATA_LICENSE = os.environ.get("STATA_LICENSE")
STATA_LICENSE_REPO = os.environ.get(
    "STATA_LICENSE_REPO",
    "https://github.com/opensafely/server-instructions.git",
)


ACTIONS_GITHUB_ORG = "opensafely-actions"
ACTIONS_GITHUB_ORG_URL = f"https://github.com/{ACTIONS_GITHUB_ORG}"

ALLOWED_GITHUB_ORGS = (
    os.environ.get("ALLOWED_GITHUB_ORGS", "opensafely").strip().split(",")
)

# We hardcode this for now, as from a security perspective, we do not want it
# to be run time configurable. Though we do override this in `local_run.py` as
# we don't want to push traffic via the proxy when running locally.
GIT_PROXY_DOMAIN = "github-proxy.opensafely.org"


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


STATS_POLL_INTERVAL = float(os.environ.get("STATS_POLL_INTERVAL", "10"))
MAINTENANCE_POLL_INTERVAL = float(
    os.environ.get("MAINTENANCE_POLL_INTERVAL", "300")
)  # 5 min
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


DEFAULT_JOB_CPU_COUNT = float(os.environ.get("DEFAULT_JOB_CPU_COUNT", 2))
DEFAULT_JOB_MEMORY_LIMIT = os.environ.get("DEFAULT_JOB_MEMORY_LIMIT", "4G")


EXECUTOR = os.environ.get("EXECUTOR", "jobrunner.executors.local:LocalDockerAPI")

# LocalDockerAPI executor specific configuration
# Note: the local backend also reuses the main GIT_REPO_DIR config

LOCAL_VOLUME_API = os.environ.get(
    "LOCAL_VOLUME_API", "jobrunner.executors.volumes:DockerVolumeAPI"
)

HIGH_PRIVACY_STORAGE_BASE = Path(
    os.environ.get("HIGH_PRIVACY_STORAGE_BASE", WORKDIR / "high_privacy")
)
MEDIUM_PRIVACY_STORAGE_BASE = Path(
    os.environ.get("MEDIUM_PRIVACY_STORAGE_BASE", WORKDIR / "medium_privacy")
)

HIGH_PRIVACY_WORKSPACES_DIR = HIGH_PRIVACY_STORAGE_BASE / "workspaces"
MEDIUM_PRIVACY_WORKSPACES_DIR = MEDIUM_PRIVACY_STORAGE_BASE / "workspaces"
JOB_LOG_DIR = HIGH_PRIVACY_STORAGE_BASE / "logs"
HIGH_PRIVACY_ARCHIVE_DIR = Path(
    os.environ.get("HIGH_PRIVACY_ARCHIVE_DIR", HIGH_PRIVACY_STORAGE_BASE / "archives")
)

# Automatically delete containers and volumes after they have been used
CLEAN_UP_DOCKER_OBJECTS = True

# use to checkout the repo
TMP_DIR = WORKDIR / "temp"

# docker specific exit codes we understand
DOCKER_EXIT_CODES = {
    # 137 = 128+9, which means was killed by signal 9, SIGKILL
    # This could be killed externally by an admin, or terminated through the
    # cancellation process.
    # Note: this can also mean killed by OOM killer, if the value of OOMKilled
    # is incorrect (as sometimes recently observed)
    137: "Job killed by OpenSAFELY admin or memory limits",
}

# BindMountVolumeAPI config
#
# used to store directories to be mounted into jobs with the BindMountVolumeAPI
HIGH_PRIVACY_VOLUME_DIR = Path(
    os.environ.get(
        "HIGH_PRIVACY_VOLUME_DIR",
        HIGH_PRIVACY_STORAGE_BASE / "volumes",
    )
)

# when running inside a docker container and using the BindMountVolumeAPI, this
# needs to point to the path to the HIGH_PRIVACY_VOLUME_DIR from the *hosts*
# perspective, as that's what docker will be looking for.
DOCKER_HOST_VOLUME_DIR = os.environ.get("DOCKER_HOST_VOLUME_DIR")

# These are currently only used with the BindMountVolumeAPI.
# It could work with DockerVolumeAPI if we can workaround docker cp only
# writing files into containers as root.
if sys.platform == "linux":
    DOCKER_USER_ID = os.environ.get("DOCKER_USER_ID", str(os.geteuid()))
    DOCKER_GROUP_ID = os.environ.get("DOCKER_GROUP_ID", str(os.getegid()))
else:
    DOCKER_USER_ID = None
    DOCKER_GROUP_ID = None


# The name of a Docker network configured to allow access to just the database and
# nothing else. Setup and configuration of this network is expected to be managed
# externally. See:
# https://github.com/opensafely-core/backend-server/pull/105
DATABASE_ACCESS_NETWORK = os.environ.get("DATABASE_ACCESS_NETWORK", "jobrunner-db")
