"""
Run any pending database migrations
"""

import argparse
import sys
from pathlib import Path

from controller import config
from jobrunner.lib import database, log_utils


def main(dbpath):
    database.ensure_db(dbpath, verbose=True)


def add_parser_args(parser):
    parser.add_argument(
        "--dbpath",
        type=Path,
        default=config.DATABASE_FILE,
        help="db file to migrate (defaults to configured db)",
    )


def run(argv):
    log_utils.configure_logging()
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    add_parser_args(parser)
    args = parser.parse_args(argv)
    main(args.dbpath)


if __name__ == "__main__":
    run(sys.argv[1:])
