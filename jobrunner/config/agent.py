import os
import sys
from pathlib import Path

from jobrunner.config import common


class ConfigException(Exception):
    pass


METRICS_FILE = common.WORKDIR / "metrics.sqlite"

# valid archive formats
ARCHIVE_FORMATS = (".tar.gz", ".tar.zstd", ".tar.xz")

BACKEND = os.environ.get("BACKEND")
# this is tested in tests/test_config.py but via subprocess so it isn't registered by coverage
if BACKEND and BACKEND not in common.BACKENDS:  # pragma: no cover
    valid_backends = ", ".join(common.BACKENDS)
    raise RuntimeError(
        f"BACKEND {BACKEND} is not valid, allowed backends are: {valid_backends}"
    )

truthy = ("true", "1", "yes")

if os.environ.get("USING_DUMMY_DATA_BACKEND", "false").lower().strip() in truthy:
    USING_DUMMY_DATA_BACKEND = True
else:
    # this branch is tested in tests/test_config.py but via subprocess so it isn't registered by coverage
    USING_DUMMY_DATA_BACKEND = False  # pragma: no cover


# Agent only; the controller passes database name only in env, agent constructs the DB url
# from [NAME]_DATABASE_URL env variables available only inside the backend
def database_urls_from_env(env):
    return {
        db_name: db_url
        for db_name, db_url in [
            (db_name, env.get(f"{db_name.upper()}_DATABASE_URL"))
            for db_name in common.VALID_DATABASE_NAMES
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
    raise ConfigException(  # pragma: no cover
        "Both PRESTO_TLS_KEY_PATH and PRESTO_TLS_CERT_PATH must be defined if either are"
    )

if PRESTO_TLS_KEY_PATH:  # pragma: no cover
    key_path = Path(PRESTO_TLS_KEY_PATH)
    if key_path.exists():
        PRESTO_TLS_KEY = key_path.read_text()
    else:
        raise ConfigException(
            f"PRESTO_TLS_KEY_PATH={key_path}, but file does not exist"
        )

if PRESTO_TLS_CERT_PATH:  # pragma: no cover
    cert_path = Path(PRESTO_TLS_CERT_PATH)
    if cert_path.exists():
        PRESTO_TLS_CERT = cert_path.read_text()
    else:
        raise ConfigException(
            f"PRESTO_TLS_CERT_PATH={cert_path}, but file does not exist"
        )


STATS_POLL_INTERVAL = float(os.environ.get("STATS_POLL_INTERVAL", "10"))

EXECUTOR = os.environ.get("EXECUTOR", "jobrunner.executors.local:LocalDockerAPI")

# LocalDockerAPI executor specific configuration
# Note: the local backend also reuses the main GIT_REPO_DIR config

HIGH_PRIVACY_STORAGE_BASE = Path(
    os.environ.get("HIGH_PRIVACY_STORAGE_BASE", common.WORKDIR / "high_privacy")
)
assert HIGH_PRIVACY_STORAGE_BASE.is_absolute()

MEDIUM_PRIVACY_STORAGE_BASE = Path(
    os.environ.get("MEDIUM_PRIVACY_STORAGE_BASE", common.WORKDIR / "medium_privacy")
)
assert MEDIUM_PRIVACY_STORAGE_BASE.is_absolute()

HIGH_PRIVACY_WORKSPACES_DIR = HIGH_PRIVACY_STORAGE_BASE / "workspaces"
MEDIUM_PRIVACY_WORKSPACES_DIR = MEDIUM_PRIVACY_STORAGE_BASE / "workspaces"
JOB_LOG_DIR = HIGH_PRIVACY_STORAGE_BASE / "logs"
HIGH_PRIVACY_ARCHIVE_DIR = Path(
    os.environ.get("HIGH_PRIVACY_ARCHIVE_DIR", HIGH_PRIVACY_STORAGE_BASE / "archives")
)

# Automatically delete containers and volumes after they have been used
CLEAN_UP_DOCKER_OBJECTS = True

# use to checkout the repo
TMP_DIR = common.WORKDIR / "temp"

# docker specific exit codes we understand
DOCKER_EXIT_CODES = {
    # 137 = 128+9, which means was killed by signal 9, SIGKILL
    # This could be killed externally by an admin, or terminated through the
    # cancellation process.
    # Note: this can also mean killed by OOM killer, if the value of OOMKilled
    # is incorrect (as sometimes recently observed)
    137: "Job killed by OpenSAFELY admin or memory limits",
}

# used to store directories to be mounted into jobs
HIGH_PRIVACY_VOLUME_DIR = Path(
    os.environ.get(
        "HIGH_PRIVACY_VOLUME_DIR",
        HIGH_PRIVACY_STORAGE_BASE / "volumes",
    )
)

# when running inside a docker container, this needs to point to the path to
# the HIGH_PRIVACY_VOLUME_DIR from the *hosts* perspective, as that's what
# docker will be looking for.
DOCKER_HOST_VOLUME_DIR = os.environ.get("DOCKER_HOST_VOLUME_DIR")

if sys.platform == "linux":
    DOCKER_USER_ID = os.environ.get("DOCKER_USER_ID", str(os.geteuid()))
    DOCKER_GROUP_ID = os.environ.get("DOCKER_GROUP_ID", str(os.getegid()))
else:  # pragma: no cover
    DOCKER_USER_ID = None
    DOCKER_GROUP_ID = None


# The name of a Docker network configured to allow access to just the database and
# nothing else. Setup and configuration of this network is expected to be managed
# externally. See:
# https://github.com/opensafely-core/backend-server/pull/105
DATABASE_ACCESS_NETWORK = os.environ.get("DATABASE_ACCESS_NETWORK", "jobrunner-db")

TASK_API_ENDPOINT = os.environ.get("CONTROLLER_TASK_API_ENDPOINT")
