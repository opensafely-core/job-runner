import configparser
import os
import re
from multiprocessing import cpu_count
from pathlib import Path


class ConfigException(Exception):
    pass


default_work_dir = Path(__file__) / "../../workdir"

WORK_DIR = Path(os.environ.get("WORK_DIR", default_work_dir)).resolve()

TMP_DIR = WORK_DIR / "temp"

GIT_REPO_DIR = WORK_DIR / "repos"

DATABASE_FILE = WORK_DIR / "db.sqlite"
DATABASE_SCHEMA_FILE = Path(__file__).parent / "schema.sql"

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

truthy = ("true", "1", "yes")

if os.environ.get("USING_DUMMY_DATA_BACKEND", "false").lower().strip() in truthy:
    USING_DUMMY_DATA_BACKEND = True
else:
    USING_DUMMY_DATA_BACKEND = BACKEND == "expectations"

ALLOWED_IMAGES = {
    "cohortextractor",
    "cohortextractor-v2",
    "stata-mp",
    "r",
    "jupyter",
    "python",
}

DOCKER_REGISTRY = "ghcr.io/opensafely-core"

DATABASE_URLS = {
    "full": os.environ.get("FULL_DATABASE_URL"),
    "slice": os.environ.get("SLICE_DATABASE_URL"),
    "dummy": os.environ.get("DUMMY_DATABASE_URL"),
}

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

# This is a crude mechanism for preventing a single large JobRequest with lots
# of associated Jobs from hogging all the resources. We want this configurable
# because it's useful to be able to disable this during tests and when running
# locally
RANDOMISE_JOB_ORDER = True

# Automatically delete containers and volumes after they have been used
CLEAN_UP_DOCKER_OBJECTS = True

# See `manage_jobs.ensure_overwritable` for more detail
ENABLE_PERMISSIONS_WORKAROUND = bool(os.environ.get("ENABLE_PERMISSIONS_WORKAROUND"))

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

# we hardcode this for now, as from a security perspective, we do not want it
# to be run time configurable.
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


STATS_DATABASE_FILE = os.environ.get("STATS_DATABASE_FILE")
if STATS_DATABASE_FILE:
    STATS_DATABASE_FILE = Path(STATS_DATABASE_FILE)

STATS_POLL_INTERVAL = float(os.environ.get("STATS_POLL_INTERVAL", "10"))


# feature flag to enable new API abstraction
EXECUTION_API = os.environ.get("EXECUTION_API", "false").lower() == "true"
