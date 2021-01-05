import os
from pathlib import Path
from multiprocessing import cpu_count


def getenv(name, default=None, env=os.environ):
    value = env.get(name)
    if value is None:
        value = env.get("OPENSAFELY_" + name)
    if value is None:
        return default
    else:
        return value


default_work_dir = Path(__file__) / "../../workdir"

WORK_DIR = Path(getenv("WORK_DIR", default_work_dir)).resolve()

TMP_DIR = WORK_DIR / "temp"

GIT_REPO_DIR = WORK_DIR / "repos"

DATABASE_FILE = WORK_DIR / "db.sqlite"

HIGH_PRIVACY_STORAGE_BASE = Path(
    getenv("HIGH_PRIVACY_STORAGE_BASE", WORK_DIR / "high_privacy")
)
MEDIUM_PRIVACY_STORAGE_BASE = Path(
    getenv("MEDIUM_PRIVACY_STORAGE_BASE", WORK_DIR / "medium_privacy")
)

HIGH_PRIVACY_WORKSPACES_DIR = HIGH_PRIVACY_STORAGE_BASE / "workspaces"
MEDIUM_PRIVACY_WORKSPACES_DIR = MEDIUM_PRIVACY_STORAGE_BASE / "workspaces"

JOB_LOG_DIR = HIGH_PRIVACY_STORAGE_BASE / "logs"

JOB_SERVER_ENDPOINT = getenv(
    "JOB_SERVER_ENDPOINT", "https://jobs.opensafely.org/api/v2/"
)
JOB_SERVER_TOKEN = getenv("JOB_SERVER_TOKEN", "token")

QUEUE_USER = getenv("QUEUE_USER", "user")
QUEUE_PASS = getenv("QUEUE_PASS", "pass")

PRIVATE_REPO_ACCESS_TOKEN = getenv("PRIVATE_REPO_ACCESS_TOKEN", "")

POLL_INTERVAL = float(getenv("POLL_INTERVAL", "5"))
JOB_LOOP_INTERVAL = float(getenv("JOB_LOOP_INTERVAL", "1.0"))

BACKEND = getenv("BACKEND", "expectations")

USING_DUMMY_DATA_BACKEND = BACKEND == "expectations"

ALLOWED_IMAGES = {"cohortextractor", "stata-mp", "r", "jupyter", "python"}

DOCKER_REGISTRY = "ghcr.io/opensafely"

DATABASE_URLS = {
    "full": getenv("FULL_DATABASE_URL"),
    "slice": getenv("SLICE_DATABASE_URL"),
    "dummy": getenv("DUMMY_DATABASE_URL"),
}

TEMP_DATABASE_NAME = getenv("TEMP_DATABASE_NAME")

MAX_WORKERS = int(getenv("MAX_WORKERS") or max(cpu_count() - 1, 1))

# See `local_run.py` for more detail
LOCAL_RUN_MODE = False

# See `manage_jobs.ensure_overwritable` for more detail
ENABLE_PERMISSIONS_WORKAROUND = bool(getenv("ENABLE_PERMISSIONS_WORKAROUND"))

STATA_LICENSE = getenv("STATA_LICENCE")
STATA_LICENSE_REPO = getenv(
    "STATA_LICENCE_REPO",
    "https://github.com/opensafely/server-instructions.git",
)
