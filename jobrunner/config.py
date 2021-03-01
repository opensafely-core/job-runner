import os
from pathlib import Path
from multiprocessing import cpu_count


class ConfigException(Exception):
    pass


default_work_dir = Path(__file__) / "../../workdir"

WORK_DIR = Path(os.environ.get("WORK_DIR", default_work_dir)).resolve()

TMP_DIR = WORK_DIR / "temp"

GIT_REPO_DIR = WORK_DIR / "repos"

DATABASE_FILE = WORK_DIR / "db.sqlite"

HIGH_PRIVACY_STORAGE_BASE = Path(
    os.environ.get("HIGH_PRIVACY_STORAGE_BASE", WORK_DIR / "high_privacy")
)
MEDIUM_PRIVACY_STORAGE_BASE = Path(
    os.environ.get("MEDIUM_PRIVACY_STORAGE_BASE", WORK_DIR / "medium_privacy")
)

HIGH_PRIVACY_WORKSPACES_DIR = HIGH_PRIVACY_STORAGE_BASE / "workspaces"
MEDIUM_PRIVACY_WORKSPACES_DIR = MEDIUM_PRIVACY_STORAGE_BASE / "workspaces"

JOB_LOG_DIR = HIGH_PRIVACY_STORAGE_BASE / "logs"

JOB_SERVER_ENDPOINT = os.environ.get(
    "JOB_SERVER_ENDPOINT", "https://jobs.opensafely.org/api/v2/"
)
JOB_SERVER_TOKEN = os.environ.get("JOB_SERVER_TOKEN", "token")

PRIVATE_REPO_ACCESS_TOKEN = os.environ.get("PRIVATE_REPO_ACCESS_TOKEN", "")

POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "5"))
JOB_LOOP_INTERVAL = float(os.environ.get("JOB_LOOP_INTERVAL", "1.0"))

BACKEND = os.environ.get("BACKEND", "expectations")

USING_DUMMY_DATA_BACKEND = BACKEND == "expectations"

ALLOWED_IMAGES = {"cohortextractor", "stata-mp", "r", "jupyter", "python"}

DOCKER_REGISTRY = "ghcr.io/opensafely-core"

DATABASE_URLS = {
    "full": os.environ.get("FULL_DATABASE_URL"),
    "slice": os.environ.get("SLICE_DATABASE_URL"),
    "dummy": os.environ.get("DUMMY_DATABASE_URL"),
}

TEMP_DATABASE_NAME = os.environ.get("TEMP_DATABASE_NAME")

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

# See `local_run.py` for more detail
LOCAL_RUN_MODE = False

# Automatically delete containers and volumes after they have been used
CLEAN_UP_DOCKER_OBJECTS = True

# See `manage_jobs.ensure_overwritable` for more detail
ENABLE_PERMISSIONS_WORKAROUND = bool(os.environ.get("ENABLE_PERMISSIONS_WORKAROUND"))

STATA_LICENSE = os.environ.get("STATA_LICENSE")
STATA_LICENSE_REPO = os.environ.get(
    "STATA_LICENSE_REPO",
    "https://github.com/opensafely/server-instructions.git",
)
