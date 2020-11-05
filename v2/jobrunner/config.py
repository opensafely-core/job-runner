import os
from pathlib import Path

default_work_dir = Path(__file__) / "../../workdir"

WORK_DIR = Path(os.environ.get("WORK_DIR", default_work_dir)).resolve()

GIT_REPO_DIR = WORK_DIR / "repos"

DATABASE_FILE = WORK_DIR / "db.sqlite"

PRIVATE_REPO_ACCESS_TOKEN = os.environ.get("PRIVATE_REPO_ACCESS_TOKEN", "")

JOB_LOOP_INTERVAL = float(os.environ.get("JOB_LOOP_INTERVAL", "0.5"))

BACKEND = os.environ.get("BACKEND", "expectations")

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
