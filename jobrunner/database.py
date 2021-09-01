"""
Super-crude ORM layer than works with dataclasses and implements just the bare
minimum of database functions we need. There was some discussion earlier about
avoiding heavywieght external dependencies like SQLAlchemy hence this little
piece of NIH-ism. However, given that we're going to be relying on external
dependencies for YAML parsing it might make sense to replace this with
something like SQLAlchemny, pinned to a known compromise-free version. The API
surface area of this module is sufficiently small that swapping it out
shouldn't be too large a job.
"""
import dataclasses
import json
import sqlite3
import threading
from enum import Enum

from jobrunner import config

CONNECTION_CACHE = threading.local()


def insert(item):
    table = item.__tablename__
    fields = dataclasses.fields(item)
    columns = ", ".join(escape(field.name) for field in fields)
    placeholders = ", ".join(["?"] * len(fields))
    sql = f"INSERT INTO {escape(table)} ({columns}) VALUES({placeholders})"
    get_connection().execute(sql, encode_field_values(fields, item))


def update(item, update_fields=None):
    assert item.id
    if update_fields is None:
        update_fields = [f.name for f in dataclasses.fields(item)]
    update_dict = {f: getattr(item, f) for f in update_fields}
    update_where(item.__class__, update_dict, id=item.id)


def update_where(itemclass, update_dict, **query_params):
    table = itemclass.__tablename__
    fields = [f for f in dataclasses.fields(itemclass) if f.name in update_dict]
    assert len(fields) == len(update_dict)
    updates = ", ".join(f"{escape(field.name)} = ?" for field in fields)
    update_params = encode_field_values(fields, update_dict)
    where, where_params = query_params_to_sql(query_params)
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


def count_where(itemclass, **query_params):
    table = itemclass.__tablename__
    where, params = query_params_to_sql(query_params)
    sql = f"SELECT COUNT(*) FROM {escape(table)} WHERE {where}"
    cursor = get_connection().execute(sql, params)
    return cursor.fetchone()[0]


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


def get_connection():
    # The caching below means we get the same connection to the database every
    # time which is done not so much for efficiency as so that we can easily
    # implement transaction support without having to explicitly pass round a
    # connection object. This is done on a per-thread basis to avoid potential
    # threading issues.
    filename = config.DATABASE_FILE
    # Looks icky but is documented `threading.local` usage
    cache = CONNECTION_CACHE.__dict__
    if filename in cache:
        return cache[filename]
    else:
        connection = get_connection_from_file(filename)
        cache[filename] = connection
        return connection


def get_connection_from_file(filename):
    if str(filename).startswith(":memory:"):
        filename = ":memory:"
    else:
        filename.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(filename)
    # Enable autocommit so changes made outside of a transaction still get
    # persisted to disk. We can use explicit transactions when we need
    # atomicity.
    conn.isolation_level = None
    # Support dict-like access to rows
    conn.row_factory = sqlite3.Row
    schema_count = list(conn.execute("SELECT COUNT(*) FROM sqlite_master"))[0][0]
    if schema_count == 0:
        with open(config.DATABASE_SCHEMA_FILE) as f:
            schema_sql = f.read()
        conn.executescript(schema_sql)
    return conn


def query_params_to_sql(params):
    """
    Turn a dict of query parameters into a pair of (SQL string, SQL values).
    All parameters are implicitly ANDed together, and there's a bit of magic to
    handle `field__in=list_of_values` queries, LIKE queries and Enum classes.
    """
    parts = []
    values = []
    for key, value in params.items():
        if key.endswith("__in"):
            field = key[:-4]
            placeholders = ", ".join(["?"] * len(value))
            parts.append(f"{escape(field)} IN ({placeholders})")
            values.extend(value)
        elif key.endswith("__like"):
            field = key[:-6]
            parts.append(f"{escape(field)} LIKE ?")
            values.append(value)
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
    Takes a list of dataclass fields and a dataclass instance or dict and
    returns the field values as a list with the appropriate conversions applied
    """
    values = []
    get_value = getattr if not isinstance(item, dict) else dict.__getitem__
    for field in fields:
        value = get_value(item, field.name)
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
