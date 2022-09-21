"""
Run any pending database migrations
"""
import argparse
from pathlib import Path

from jobrunner import config
from jobrunner.lib import database, log_utils


def run():
    log_utils.configure_logging()
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument(
        "--dbpath",
        type=Path,
        default=config.DATABASE_FILE,
        help="db file to migrate (defaults to configured db)",
    )
    args = parser.parse_args()
    database.ensure_db(args.dbpath)


if __name__ == "__main__":
    run()
