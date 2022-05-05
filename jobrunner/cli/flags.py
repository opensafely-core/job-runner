"""
Ops utility getting and setting db flags
"""
import argparse
import sqlite3
import sys

from jobrunner.lib.database import create_table, get_connection, select_values
from jobrunner.models import Flag
from jobrunner.queries import get_flag, set_flag


def parse_cli_flag(raw):
    """Do you have a flaaaaaag?"""
    name, equals, value = raw.partition("=")
    if not equals:
        raise argparse.ArgumentError(f"set must have for {name}=value, not just {name}")
    if value == "":
        value = None
    return name, value


def set_flags(flags, create):
    try:
        for name, value in flags:
            set_flag(name, value)

    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            if create:
                create_table(get_connection(), Flag)
                # try again
                for name, value in flags:
                    set_flag(name, value)
            else:
                sys.exit(
                    "Flags table does not exists. Run command again with --create to create it."
                )
        else:
            raise

    return [p[0] for p in flags]


def main(action, flags, create=False):
    if action == "set":
        flags = set_flags(flags, create)
    elif not flags:
        try:
            flags = select_values(Flag, "id")
        except sqlite3.OperationalError:
            pass

    for flag in flags:
        print(f"{flag}={get_flag(flag)}")


def run(argv):
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    subparsers = parser.add_subparsers(help="get or set")

    parser_get = subparsers.add_parser("get", help="get the current values of flags")
    parser_get.add_argument(
        "flags", nargs="*", help="flags to get, or empty for all flags"
    )
    parser_get.set_defaults(action="get")

    parser_set = subparsers.add_parser("set", help="set flag values")
    parser_set.add_argument(
        "flags",
        nargs="+",
        type=parse_cli_flag,
        metavar="FLAG=[VALUE]",
        help="Flags to set",
    )
    parser_set.add_argument(
        "--create",
        action="store_true",
        help="Create the flags DB schema if missing",
    )
    parser_set.set_defaults(action="set")

    args = parser.parse_args(argv)
    main(**vars(args))


if __name__ == "__main__":
    run(sys.argv[1:])
