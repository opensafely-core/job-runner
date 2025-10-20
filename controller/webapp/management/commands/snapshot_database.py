import datetime
import sqlite3
import textwrap

from django.core.management.base import BaseCommand

from controller import config


class Command(BaseCommand):
    """\
    Takes a snapshot of the database, writes it as timestamped file to the configured
    BACKUPS_PATH directory, and removes any previously created backup files.
    """

    help = textwrap.dedent(__doc__)

    def handle(self, **options):
        backup_database(config.DATABASE_FILE, config.BACKUPS_PATH)


def backup_database(sqlite_file, backups_path):
    if not backups_path.exists():
        raise RuntimeError(
            f"BACKUPS_PATH does not exist: {backups_path}\n"
            f"Refusing to run on the assumption that backups are misconfigured."
        )

    now = datetime.datetime.now(datetime.timezone.utc)
    existing_files = list(backups_path.glob("db.snapshot_*.sqlite"))
    target_file = backups_path / f"db.snapshot_{now:%Y-%m-%d_%H%M%S}Z.sqlite"
    temp_file = target_file.with_name(f"{target_file.name}.tmp.sqlite")

    read_conn = sqlite3.connect(sqlite_file.absolute().as_uri() + "?mode=ro", uri=True)
    write_conn = sqlite3.connect(temp_file)
    read_conn.backup(write_conn)
    write_conn.close()

    temp_file.replace(target_file)

    # We don't need historical files because the volume we're backing up into is itself
    # backed up, with historical versions kept appropriately
    for f in existing_files:
        f.unlink()
