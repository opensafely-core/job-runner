import os
from pathlib import Path


# A list of known available backends
# The controller uses this to:
# - validate each job is from a known backend
# - looping through BACKENDS in sync
# The agent uses it to validate its BACKEND config
BACKENDS = os.environ.get("BACKENDS", "test,tpp,emis").strip().split(",")

# Used for tracing in both agent and controller
# This refers to a file created in the docker image by Dockerfile
JOBRUNNER_VERSION_FILE_PATH = Path(
    os.environ.get("JOBRUNNER_VERSION_FILE_PATH", "/app/metadata/version.txt")
)
if JOBRUNNER_VERSION_FILE_PATH.exists():
    # this is tested in tests/test_config.py but via subprocess so it isn't registered by coverage
    jobrunner_version = (
        JOBRUNNER_VERSION_FILE_PATH.read_text().rstrip()
    )  # pragma: no cover
else:
    jobrunner_version = "unknown"
VERSION = os.environ.get("VERSION", jobrunner_version)

# Used by controller to build full image
# Used by agent for interacting with volumes in docker.py (outside of a job/task)
DOCKER_REGISTRY = os.environ.get("DOCKER_REGISTRY", "ghcr.io/opensafely-core")

# Used for job/task loop timing
JOB_LOOP_INTERVAL = float(os.environ.get("JOB_LOOP_INTERVAL", "1.0"))

# Local storage
# Note: both agent and controller need to checkout git repos locally
# Agent: for repos, results and logs
# Controller: for db files and repos
default_work_dir = Path(__file__).parents[1] / "workdir"
WORKDIR = Path(os.environ.get("WORKDIR", default_work_dir)).resolve()
GIT_REPO_DIR = WORKDIR / "repos"

GITHUB_PROXY_DOMAIN = os.environ.get(
    "GITHUB_PROXY_DOMAIN", "github-proxy.opensafely.org"
)
PRIVATE_REPO_ACCESS_TOKEN = os.environ.get("PRIVATE_REPO_ACCESS_TOKEN", "")

# Used by the controller to validate database name passed in a job request
# Used by the agent to build database URLS
VALID_DATABASE_NAMES = ["default", "include_t1oo"]

# What github organisations we are allowed to checkout code from
ALLOWED_GITHUB_ORGS = (
    os.environ.get("ALLOWED_GITHUB_ORGS", "opensafely,opensafely-actions")
    .strip()
    .split(",")
)
