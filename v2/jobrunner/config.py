import os
from pathlib import Path
from multiprocessing import cpu_count


default_work_dir = Path(__file__) / "../../workdir"

WORK_DIR = Path(os.environ.get("WORK_DIR", default_work_dir)).resolve()

TMP_DIR = WORK_DIR / "temp"

GIT_REPO_DIR = WORK_DIR / "repos"

DATABASE_FILE = WORK_DIR / "db.sqlite"

PRIVATE_REPO_ACCESS_TOKEN = os.environ.get("PRIVATE_REPO_ACCESS_TOKEN", "")

JOB_LOOP_INTERVAL = float(os.environ.get("JOB_LOOP_INTERVAL", "0.5"))

BACKEND = os.environ.get("BACKEND", "expectations")

USING_DUMMY_DATA_BACKEND = BACKEND == "expectations"

ALLOWED_IMAGES = {"cohortextractor", "stata-mp", "r", "jupyter"}

DOCKER_REGISTRY = "docker.opensafely.org"

HIGH_PRIVACY_STORAGE_BASE = Path(
    os.environ.get("HIGH_PRIVACY_STORAGE_BASE", WORK_DIR / "high_privacy")
)
MEDIUM_PRIVACY_STORAGE_BASE = Path(
    os.environ.get("MEDIUM_PRIVACY_STORAGE_BASE", WORK_DIR / "medium_privacy")
)

HIGH_PRIVACY_WORKSPACES_DIR = HIGH_PRIVACY_STORAGE_BASE / "workspaces"
MEDIUM_PRIVACY_WORKSPACES_DIR = MEDIUM_PRIVACY_STORAGE_BASE / "workspaces"

JOB_LOG_DIR = HIGH_PRIVACY_STORAGE_BASE / "logs"

DATABASE_URLS = {
    "full": os.environ.get("FULL_DATABASE_URL"),
    "slice": os.environ.get("SLICE_DATABASE_URL"),
    "dummy": os.environ.get("DUMMY_DATABASE_URL"),
}

TEMP_DATABASE_NAME = os.environ.get("TEMP_DATABASE_NAME")

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", max(cpu_count() - 1, 1)))
