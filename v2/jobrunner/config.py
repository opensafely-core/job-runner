import os
from pathlib import Path

default_work_dir = Path(__file__) / "../../workdir"

WORK_DIR = Path(os.environ.get("WORK_DIR", default_work_dir)).resolve()

GIT_REPO_DIR = WORK_DIR / "repos"

DATABASE_FILE = WORK_DIR / "db.sqlite"

PRIVATE_REPO_ACCESS_TOKEN = os.environ.get("PRIVATE_REPO_ACCESS_TOKEN", "")

# The keys of this dictionary are all the supported `run` commands in
# jobs
RUN_COMMANDS = {
    "cohortextractor": {
        "docker_invocation": ["docker.opensafely.org/cohortextractor"],
    },
    "stata-mp": {"docker_invocation": ["docker.opensafely.org/stata-mp"]},
    "r": {"docker_invocation": ["docker.opensafely.org/r"]},
    "jupyter": {"docker_invocation": ["docker.opensafely.org/jupyter"]},
}

JOB_LOOP_INTERVAL = float(os.environ.get("JOB_LOOP_INTERVAL", "0.5"))

BACKEND = os.environ.get("BACKEND", "expectations")
