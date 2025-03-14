"""
Run any pending database migrations
"""

import argparse
import sys
from pathlib import Path

from jobrunner import config
from jobrunner.lib import database, log_utils


def run(argv):
    log_utils.configure_logging()
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument(
        "--dbpath",
        type=Path,
        default=config.DATABASE_FILE,
        help="db file to migrate (defaults to configured db)",
    )
    args = parser.parse_args(argv)
    database.ensure_db(args.dbpath, verbose=True)


if __name__ == "__main__":
    run(sys.argv[1:])
