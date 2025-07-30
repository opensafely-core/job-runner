"""
Ops utility getting and setting db flags
"""

import argparse
import sqlite3
import sys

from controller.cli.utils import add_backend_argument
from controller.lib.database import create_table, get_connection
from controller.models import Flag
from controller.queries import get_current_flags, get_flag, set_flag


def parse_cli_flag(raw):
    """Do you have a flaaaaaag?"""
    name, equals, value = raw.partition("=")
    if not equals:
        raise argparse.ArgumentError(f"set must have for {name}=value, not just {name}")
    if value == "":
        value = None
    return name, value


def main(backend, action, flags, create=False):
    try:
        get_current_flags(backend)
    except sqlite3.OperationalError as e:  # pragma: no cover
        if "no such table" in str(e):
            if create:
                create_table(get_connection(), Flag)
                get_current_flags(backend)
            else:
                sys.exit(
                    "The flags table does not exists. Run command again with --create to create it."
                )

    flags_to_show = []

    if action == "set":
        for name, value in flags:
            flag = set_flag(name, value, backend)
            flags_to_show.append(flag)

    else:  # action == "get"
        if flags:
            for f in flags:
                try:
                    flag = get_flag(f, backend)
                except ValueError:
                    flag = Flag(f, None, backend, None)
                flags_to_show.append(flag)
        else:
            flags_to_show = get_current_flags(backend)

    for flag in flags_to_show:
        print(flag)


def add_parser_args(parser):
    subparsers = parser.add_subparsers(dest="action")
    subparsers.required = True
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
    add_backend_argument(parser_get, helptext="backend this flag/flags relates to")
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
    add_backend_argument(parser_set, helptext="backend this flag/flags relates to")
    parser_set.set_defaults(action="set")


def run(argv):
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    add_parser_args(parser)

    args = parser.parse_args(argv)
    main(**vars(args))


if __name__ == "__main__":
    run(sys.argv[1:])  # pragma: no cover
