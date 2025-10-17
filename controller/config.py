import configparser
import os
import re
from pathlib import Path

import pipeline

from common import config as common_config


class ConfigException(Exception):
    pass


DATABASE_FILE = common_config.WORKDIR / "db.sqlite"

BACKUPS_PATH = Path(os.environ.get("BACKUPS_PATH", common_config.WORKDIR / "backups"))

JOB_SERVER_ENDPOINT = os.environ.get(
    "JOB_SERVER_ENDPOINT", "https://jobs.opensafely.org/api/v2/"
)

JOB_SERVER_TOKENS = {
    backend: os.environ.get(f"{backend.upper()}_JOB_SERVER_TOKEN", "token")
    for backend in common_config.BACKENDS
}


# Build a dict of backends which each client token is allowed to request information for
def client_tokens_from_env(env):
    tokens_per_backend = {}
    for backend in common_config.BACKENDS:
        client_tokens = env.get(f"{backend.upper()}_CLIENT_TOKENS")
        if client_tokens:
            tokens_per_backend[backend] = [
                client_token for client_token in client_tokens.split(",")
            ]
        else:
            tokens_per_backend[backend] = []

    backends_per_client_token = {}
    for backend, client_tokens in tokens_per_backend.items():
        for client_token in client_tokens:
            backends_per_client_token.setdefault(client_token, []).append(backend)
    return backends_per_client_token


CLIENT_TOKENS = client_tokens_from_env(os.environ)


# API poll
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "5"))

# TICK trace interval
TICK_POLL_INTERVAL = float(os.environ.get("TICK_POLL_INTERVAL", "30"))

ALLOWED_IMAGES = {
    "ehrql",
    "stata-mp",
    "r",
    "jupyter",
    "python",
    "sqlrunner",
}

# Set workers per-backend. This will be used by the controller to
# determine if there are enough resources available to start a new
# job running.
default_workers = {"test": 2, "tpp": 10, "emis": 10}
MAX_WORKERS = {
    backend: int(
        os.environ.get(f"{backend.upper()}_MAX_WORKERS")
        or default_workers.get(backend, 10)
    )
    for backend in common_config.BACKENDS
}
MAX_DB_WORKERS = {
    backend: int(
        os.environ.get(f"{backend.upper()}_MAX_DB_WORKERS") or MAX_WORKERS[backend]
    )
    for backend in common_config.BACKENDS
}

# Currently we assume all backends will have the same
# limits on L4 files
LEVEL4_MAX_FILESIZE = int(
    os.environ.get("LEVEL4_MAX_FILESIZE", 16 * 1024 * 1024)
)  # 16mb
LEVEL4_MAX_CSV_ROWS = int(os.environ.get("LEVEL4_MAX_CSV_ROWS", 5000))
LEVEL4_FILE_TYPES = list(sorted(pipeline.constants.LEVEL4_FILE_TYPES))

STATA_LICENSE = os.environ.get("STATA_LICENSE")

ACTIONS_GITHUB_ORG = "opensafely-actions"
ACTIONS_GITHUB_ORG_URL = f"https://github.com/{ACTIONS_GITHUB_ORG}"


def parse_job_resource_weights(config_file_template):
    """
    Parse a simple ini file per backend which looks like this:

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
    weights = {}
    for backend in common_config.BACKENDS:
        weights[backend] = {}
        config_file = common_config.WORKDIR / Path(
            config_file_template.format(backend=backend.lower())
        )
        if config_file.exists():
            config = configparser.ConfigParser()
            config.read_string(config_file.read_text(), source=str(config_file))
            for workspace in config.sections():
                weights[backend][workspace] = {
                    re.compile(pattern): float(weight)
                    for (pattern, weight) in config.items(workspace)
                }
    return weights


JOB_RESOURCE_WEIGHTS = parse_job_resource_weights("job-resource-weights_{backend}.ini")

MAINTENANCE_POLL_INTERVAL = float(
    os.environ.get("MAINTENANCE_POLL_INTERVAL", "300")
)  # 5 min
MAINTENANCE_ENABLED_BACKENDS = (
    os.environ.get("MAINTENANCE_ENABLED_BACKENDS", "tpp").strip().split(",")
)

# Map known exit codes to user-friendly messages
DATABASE_EXIT_CODES = {
    # Custom database-related exit codes return from ehrQL, see e.g.
    # https://github.com/opensafely-core/ehrql/blob/889dcfd9762b/ehrql/backends/tpp.py#L159-L171
    3: (
        "A transient database error occurred, your job may run "
        "if you try it again, if it keeps failing then contact tech support"
    ),
    4: "New data is being imported into the database, please try again in a few hours",
    5: "Something went wrong with the database, please contact tech support",
    # Other ehrQL exit codes with specific meanings, see:
    # https://github.com/opensafely-core/ehrql/blob/e0c47acdb887/ehrql/__main__.py#L123-L134
    10: "There was a problem reading your ehrQL code; please confirm that it runs locally",
    11: "There was a problem reading one of the supplied data files",
    12: "You do not have the required permissions for the ehrQL you are trying to run",
}


# per-backend job limits
def job_limits_from_env(env, limit_name, default, transform=str):
    common_default = transform(env.get(f"DEFAULT_{limit_name.upper()}") or default)
    return {
        backend: transform(
            env.get(f"{backend.upper()}_{limit_name.upper()}") or common_default
        )
        for backend in common_config.BACKENDS
    }


DEFAULT_JOB_CPU_COUNT = job_limits_from_env(os.environ, "job_cpu_count", 2, float)
DEFAULT_JOB_MEMORY_LIMIT = job_limits_from_env(os.environ, "job_memory_limit", "4G")


# Repos associated with projects approved by NOD to try out the new Event Level Data
# features in ehrQL against real data
REPOS_WITH_EHRQL_EVENT_LEVEL_ACCESS = {
    # Vaccine effectiveness repos
    "https://github.com/opensafely/ve-ccw",
    "https://github.com/opensafely/covid-vaccine-history",
}
