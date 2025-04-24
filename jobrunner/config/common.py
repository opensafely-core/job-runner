import os
from pathlib import Path


# Used for tracing in both agent and controller
VERSION = os.environ.get("VERSION", "")

# Used by controller to build full image
# Used by agent for interacting with volumes in docker.py (outside of a job/task)
DOCKER_REGISTRY = os.environ.get("DOCKER_REGISTRY", "ghcr.io/opensafely-core")

# Used for job/task loop timing
JOB_LOOP_INTERVAL = float(os.environ.get("JOB_LOOP_INTERVAL", "1.0"))

# Local storage
# Note: both agent and controller need to checkout git repos locally
# Agent: for repos, results and logs
# Controller: for db files and repos
default_work_dir = Path(__file__) / "../../../workdir"
WORKDIR = Path(os.environ.get("WORKDIR", default_work_dir)).resolve()
GIT_REPO_DIR = WORKDIR / "repos"

# We hardcode this for now, as from a security perspective, we do not want it
# to be run time configurable
# TODO Controller will not need to proxy once outside backend
GIT_PROXY_DOMAIN = "github-proxy.opensafely.org"
PRIVATE_REPO_ACCESS_TOKEN = os.environ.get("PRIVATE_REPO_ACCESS_TOKEN", "")
