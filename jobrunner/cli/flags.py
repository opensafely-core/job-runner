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


def main(action, flags, create=False):
    try:
        current_flags = select_values(Flag, "id")
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            if create:
                create_table(get_connection(), Flag)
                current_flags = select_values(Flag, "id")
            else:
                sys.exit(
                    "The flags table does not exists. Run command again with --create to create it."
                )

    flags_to_show = []

    if action == "set":
        for name, value in flags:
            set_flag(name, value)
            flags_to_show.append(name)

    else:  # action == "get"
        if flags:
            flags_to_show = flags
        else:
            flags_to_show = current_flags

    for flag in flags_to_show:
        print(f"{flag}={get_flag(flag)}")


def run(argv):
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])

    subparsers = parser.add_subparsers()
    # for get, flag arguments is optional
    parser_get = subparsers.add_parser("get", help="get the current values of flags")
    parser_get.add_argument(
        "flags", nargs="*", help="flags to get, or empty for all flags"
    )
    parser_get.add_argument(
        "--create",
        action="store_true",
        help="Create the flags DB schema if missing",
    )
    parser_get.set_defaults(action="get")

    # for set, at least one flag argument is requires, and it must contain =
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
