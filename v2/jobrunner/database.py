import functools
import json
import os
from pathlib import Path
import sqlite3

from . import config


def insert(table, row):
    columns = ", ".join(map(escape, row.keys()))
    placeholders = ", ".join(["?"] * len(row))
    sql = f"INSERT INTO {escape(table)} ({columns}) VALUES({placeholders})"
    get_connection().execute(sql, encode_row_values(row))


def update(table, value_dict, **query_params):
    updates = ", ".join(f"{escape(column)} = ?" for column in value_dict.keys())
    update_params = list(value_dict.values())
    where, where_params = query_params_to_sql(query_params)
    get_connection().execute(
        f"UPDATE {escape(table)} SET {updates} WHERE {where}",
        update_params + where_params,
    )


def find_where(table, **query_params):
    where, params = query_params_to_sql(query_params)
    sql = f"SELECT * FROM {escape(table)} WHERE {where}"
    cursor = get_connection().execute(sql, params)
    return list(map(decode_row, cursor))


def exists_where(table, **query_params):
    where, params = query_params_to_sql(query_params)
    sql = f"SELECT EXISTS (SELECT 1 FROM {escape(table)} WHERE {where})"
    cursor = get_connection().execute(sql, params)
    return bool(cursor.fetchone()[0])


def transaction():
    # Connections function as context managers which create transactions.
    # See: https://docs.python.org/3/library/sqlite3.html#using-the-connection-as-a-context-manager
    # We're relying here on the fact that because of the lru_cache,
    # `get_connection` actually returns the same connection instance every time
    return get_connection()


@functools.lru_cache()
def get_connection():
    os.makedirs(config.DATABASE_FILE.parent, exist_ok=True)
    conn = sqlite3.connect(config.DATABASE_FILE)
    # Enable autocommit so changes made outside of a transaction still get
    # persisted to disk.  We can use explicit transactions when we need
    # atomicity.
    conn.isolation_level = None
    # Support dict-like access to rows
    conn.row_factory = sqlite3.Row
    schema_count = list(conn.execute("SELECT COUNT(*) FROM sqlite_master"))[0][0]
    if schema_count == 0:
        with open(Path(__file__).parent / "schema.sql") as f:
            schema_sql = f.read()
        conn.executescript(schema_sql)
    return conn


def query_params_to_sql(params):
    parts = []
    values = []
    for key, value in params.items():
        if key.endswith("__in"):
            field = key[:-4]
            placeholders = ", ".join(["?"] * len(value))
            parts.append(f"{escape(field)} IN ({placeholders})")
            values.extend(value)
        else:
            parts.append(f"{escape(key)} = ?")
            values.append(value)
    if not parts:
        parts = ["1 = 1"]
    return " AND ".join(parts), values


def escape(s):
    """
    Escape SQLite identifier (as opposed to string literal)
    See https://www.sqlite.org/lang_keywords.html
    """
    return '"{}"'.format(s.replace('"', '""'))


def decode_row(row):
    return {
        k: json_decode_if_not_none(row[k]) if k.endswith("_json") else row[k]
        for k in row.keys()
    }


def encode_row_values(row):
    return [
        json_encode_if_not_none(v) if k.endswith("_json") else v for k, v in row.items()
    ]


def json_encode_if_not_none(value):
    return json.dumps(value) if value is not None else None


def json_decode_if_not_none(s):
    return json.loads(s) if s is not None else None
