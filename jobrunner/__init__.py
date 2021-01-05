"""Handle env file loading an parsing as early as possible."""
from pathlib import Path
import logging
import os

EARLY_LOGS = []


def parse_env(contents):
    """Parse a simple environment file."""
    env = {}
    for line in contents.split("\n"):
        line = line.strip()
        if not line or line[0] == "#":
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def load_env(path=None, logs=EARLY_LOGS):
    if not path.exists():
        logs.append(
            (
                logging.WARNING,
                f"Could not find environment file {path}",
            )
        )
        return

    env = parse_env(path.read_text())
    if env:
        os.environ.update(env)
        logs.append(
            (
                logging.INFO,
                f"Loaded environment variables from {path}",
            )
        )
    else:
        logs.append(
            (
                logging.WARNING,
                f"Could not parse environment variables from {path}",
            )
        )


# load any env file *before* we import anything
load_env(Path(os.environ.get("ENVPATH", ".env")))
