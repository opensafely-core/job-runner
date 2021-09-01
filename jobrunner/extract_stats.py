"""
Quick script to grab an extract of resource usage stats from the stats database
"""
import argparse
import gzip
import sqlite3
from pathlib import Path

from jobrunner import config


def main(output_file, since, gz=False):
    output_file = Path(output_file)
    database_file = config.STATS_DATABASE_FILE
    assert database_file.exists()
    assert not output_file.exists()
    conn = sqlite3.connect(database_file)
    conn.execute("ATTACH DATABASE ? AS extract", [str(output_file)])
    conn.execute(
        """
        CREATE TABLE extract.containers AS SELECT
          timestamp,
          json_each.key AS container,
          json_extract(json_each.value, "$.cpu_percentage") AS cpu_percentage,
          json_extract(json_each.value, "$.memory_used") AS memory_used
        FROM
          main.stats,
          json_each(data, '$.containers')
        WHERE
           timestamp > ?
        """,
        [since],
    )
    conn.close()
    if gz:
        output_file.with_suffix(".sqlite.gz").write_bytes(
            gzip.compress(output_file.read_bytes(), compresslevel=6)
        )
        output_file.unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument("output_file", help="File to write to")
    parser.add_argument(
        "--since", help="Extract stats since this date", default="2020-01-01"
    )
    parser.add_argument("--gz", action="store_true", help="Gzip output file")
    args = parser.parse_args()
    main(**vars(args))
