import dataclasses
from enum import Enum
import functools
import json
from pathlib import Path
import sqlite3

from . import config


def insert(item):
    table = item.__tablename__
    fields = dataclasses.fields(item)
    columns = ", ".join(escape(field.name) for field in fields)
    placeholders = ", ".join(["?"] * len(fields))
    sql = f"INSERT INTO {escape(table)} ({columns}) VALUES({placeholders})"
    get_connection().execute(sql, encode_field_values(fields, item))


def update(item, update_fields):
    assert item.id
    table = item.__tablename__
    fields = [f for f in dataclasses.fields(item) if f.name in update_fields]
    assert fields
    updates = ", ".join(f"{escape(field.name)} = ?" for field in fields)
    update_params = encode_field_values(fields, item)
    where, where_params = query_params_to_sql({"id": item.id})
    get_connection().execute(
        f"UPDATE {escape(table)} SET {updates} WHERE {where}",
        update_params + where_params,
    )


def find_where(itemclass, **query_params):
    table = itemclass.__tablename__
    fields = dataclasses.fields(itemclass)
    where, params = query_params_to_sql(query_params)
    sql = f"SELECT * FROM {escape(table)} WHERE {where}"
    cursor = get_connection().execute(sql, params)
    return [itemclass(*decode_field_values(fields, row)) for row in cursor]


def exists_where(itemclass, **query_params):
    table = itemclass.__tablename__
    where, params = query_params_to_sql(query_params)
    sql = f"SELECT EXISTS (SELECT 1 FROM {escape(table)} WHERE {where})"
    cursor = get_connection().execute(sql, params)
    return bool(cursor.fetchone()[0])


def select_values(itemclass, column, **query_params):
    table = itemclass.__tablename__
    fields = [f for f in dataclasses.fields(itemclass) if f.name == column]
    assert fields
    where, params = query_params_to_sql(query_params)
    sql = f"SELECT {escape(column)} FROM {escape(table)} WHERE {where}"
    cursor = get_connection().execute(sql, params)
    return [decode_field_values(fields, row)[0] for row in cursor]


def transaction():
    # Connections function as context managers which create transactions.
    # See: https://docs.python.org/3/library/sqlite3.html#using-the-connection-as-a-context-manager
    # We're relying here on the fact that because of the lru_cache,
    # `get_connection` actually returns the same connection instance every time
    conn = get_connection()
    conn.execute("BEGIN")
    return conn


@functools.lru_cache()
def get_connection():
    config.DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.DATABASE_FILE)
    # Enable autocommit so changes made outside of a transaction still get
    # persisted to disk. We can use explicit transactions when we need
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
    # Bit of a hack: convert any Enum instances to their values so we can use
    # them in querying
    values = [v.value if isinstance(v, Enum) else v for v in values]
    if not parts:
        parts = ["1 = 1"]
    return " AND ".join(parts), values


def escape(s):
    """
    Escape SQLite identifier (as opposed to string literal)
    See https://www.sqlite.org/lang_keywords.html
    """
    return '"{}"'.format(s.replace('"', '""'))


def encode_field_values(fields, item):
    """
    Takes a list of dataclass fields and a dataclass instance and returns the
    field values as a list with the appropriate conversions applied
    """
    values = []
    for field in fields:
        value = getattr(item, field.name)
        # Dicts and lists get encoded as JSON
        if field.type in (list, dict) and value is not None:
            value = json.dumps(value)
        # Enums get encoded as their string/int values
        elif issubclass(field.type, Enum) and value is not None:
            value = value.value
        values.append(value)
    return values


def decode_field_values(fields, row):
    """
    Takes a list of dataclass fields and a SQLite row (or any dict-like) and
    returns field values as a list with the appropriate conversions applied
    """
    values = []
    for field in fields:
        value = row[field.name]
        # Dicts and lists get decoded from JSON
        if field.type in (list, dict) and value is not None:
            value = json.loads(value)
        # Enums get transformed back from their string/int values
        elif issubclass(field.type, Enum) and value is not None:
            value = field.type(value)
        values.append(value)
    return values
