"""
Tool for migrating data from manifests to the database.

Usage:

    python -m jobrunner.cli.manifest_migration --batch-size 50

Reads job records from manifest files in all workspaces and creates records in the database for any that are missing.
For jobs that already exist in the database, it doesn't make any attempt to check that they are consistent with the
record in the manifest.

The number of jobs that are created list limited by the --batch-size argument. If there are more jobs that could have
been migrated then a message is displayed when the tool exits. The operator is expected to run the tool once with a
small batch size to check that it's working correctly and then use a large batch size to complete the operation. The
tool will report when there are no further jobs to be migrated.
"""
import argparse
import sys

from jobrunner import manifest_to_database_migration


def main(args=None):
    args = args or []
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument(
        "--batch-size", type=int, default=1, help="maximum number of jobs to create"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="do a dry run migration without modifying data and carrying on if there is an error with a single job or workspace",
    )
    parsed = parser.parse_args(args)

    manifest_to_database_migration.migrate_all(
        batch_size=parsed.batch_size,
        dry_run=parsed.dry_run,
        ignore_errors=parsed.dry_run,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
