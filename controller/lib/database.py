"""
Super-crude ORM layer than works with dataclasses and implements just the bare
minimum of database functions we need. There was some discussion earlier about
avoiding heavyweight external dependencies like SQLAlchemy hence this little
piece of NIH-ism. However, given that we're going to be relying on external
dependencies for YAML parsing it might make sense to replace this with
something like SQLAlchemy, pinned to a known compromise-free version. The API
surface area of this module is sufficiently small that swapping it out
shouldn't be too large a job.
"""

import contextlib
import dataclasses
import json
import logging
import sqlite3
import threading
from enum import Enum
from pathlib import Path

from opentelemetry import trace

from controller import config


# from controller.models import State, StatusCode


log = logging.getLogger(__name__)

CONNECTION_CACHE = threading.local()
TABLES = {}
MIGRATIONS = {}


tracer = trace.get_tracer("db")


def databaseclass(cls):
    dc = dataclasses.dataclass(cls)
    assert hasattr(dc, "__tablename__"), "must have __tablename__ attribute"
    assert hasattr(dc, "__tableschema__"), "must have __tableschema__ attribute"
    fields = {f.name for f in dataclasses.fields(dc)}
    assert "id" in fields, "must have primary key 'id'"
    TABLES[dc.__tablename__] = dc
    return dc


def migration(version, sql):
    """Used to record a migration"""
    assert version not in MIGRATIONS, f"Migration {version} already exists."
    MIGRATIONS[version] = sql


def generate_insert_sql(item):
    table = item.__tablename__
    fields = dataclasses.fields(item)
    columns = ", ".join(escape(field.name) for field in fields)
    placeholders = ", ".join(["?"] * len(fields))
    sql = f"INSERT INTO {escape(table)} ({columns}) VALUES({placeholders})"
    return sql, fields


def insert(item):
    sql, fields = generate_insert_sql(item)

    get_connection().execute(sql, encode_field_values(fields, item))


def upsert(item, keys=("id",)):
    assert all(getattr(item, k) for k in keys)
    insert_sql, fields = generate_insert_sql(item)

    updates = ", ".join(f"{escape(field.name)} = ?" for field in fields)
    key_sql = ", ".join(escape(k) for k in keys)
    # Note: technically we update the id on conflict with this approach, which
    # is unnecessary, but it does not hurt and simplifies updates and params
    # parts of the query.
    sql = f"""
        {insert_sql}
        ON CONFLICT({key_sql}) DO UPDATE SET {updates}
    """
    params = encode_field_values(fields, item)
    # pass params twice, once for INSERT and once for UPDATE
    get_connection().execute(sql, params + params)


def update(item, exclude_fields=None):
    assert item.id
    exclude_fields = exclude_fields or []
    update_fields = [
        f.name for f in dataclasses.fields(item) if f.name not in exclude_fields
    ]
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


def fast_find_where(itemclass, **query_params):
    table = "job"
    fields = dataclasses.fields(itemclass)
    where, params = query_params_to_sql(query_params)
    sql = f"SELECT * FROM {escape(table)} WHERE {where}"
    cursor = get_connection().execute(sql, params)
    return [fast_decode_field_values(fields, row, itemclass) for row in cursor]
    # return [itemclass(*decode_field_values(fields, row)) for row in cursor]


def find_where(itemclass, **query_params):
    table = itemclass.__tablename__
    fields = dataclasses.fields(itemclass)
    where, params = query_params_to_sql(query_params)
    sql = f"SELECT * FROM {escape(table)} WHERE {where}"
    cursor = get_connection().execute(sql, params)
    return [itemclass(*decode_field_values(fields, row)) for row in cursor]


def find_all(itemclass):  # pragma: nocover
    return find_where(itemclass)


def find_one(itemclass, **query_params):
    results = find_where(itemclass, **query_params)
    if len(results) == 0:
        raise ValueError(
            f"Found no {itemclass.__name__}s matching {query_params}, expecting one"
        )
    if len(results) > 1:
        raise ValueError(
            f"Found {len(results)} {itemclass.__name__}s matching {query_params}, expecting only one"
        )
    return results[0]


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


@contextlib.contextmanager
def transaction():
    # Connections function as context managers which create transactions.
    # See: https://docs.python.org/3/library/sqlite3.html#using-the-connection-as-a-context-manager
    # We want to measure it with otel, so we combine them in an ExitStack
    with contextlib.ExitStack() as stack:
        stack.enter_context(tracer.start_as_current_span("TRANSACTION"))
        # We're relying here on the fact that because of the lru_cache,
        # `get_connection` actually returns the same connection instance every
        # time
        conn = get_connection()
        conn.execute("BEGIN IMMEDIATE")
        stack.enter_context(conn)
        yield conn


def filename_or_get_default(filename=None):
    if filename is None:
        filename = config.DATABASE_FILE
        assert filename is not None, (
            "DATABASE_FILE config must not be None; did you omit the `db` fixture in a test?"
        )
    return filename


def get_connection(filename=None):
    """Return the current configured connection."""
    # The caching below means we get the same connection to the database every
    # time which is done not so much for efficiency as so that we can easily
    # implement transaction support without having to explicitly pass round a
    # connection object. This is done on a per-thread basis to avoid potential
    # threading issues.
    filename = filename_or_get_default(filename)

    # Looks icky but is documented `threading.local` usage
    cache = CONNECTION_CACHE.__dict__
    if filename not in cache:
        conn = sqlite3.connect(filename, uri=True)
        # Enable autocommit so changes made outside of a transaction still get
        # persisted to disk. We can use explicit transactions when we need
        # atomicity.
        conn.isolation_level = None
        # Support dict-like access to rows
        conn.row_factory = sqlite3.Row
        cache[filename] = conn

        # use WAL to enable other processes (e.g. operational tasks) to read the DB.
        # job-runner should be the only active writer, which means if we need
        # some other process to write the db (e.g. a backfill), then we should
        # stop job-runner.
        conn.execute("PRAGMA journal_mode=WAL")
        # These settings give much better write performance than the default
        # without sacrificing consistency guarantees
        conn.execute("PRAGMA synchronous = NORMAL")
        # How long (in ms) to let one write transaction wait for another
        conn.execute("PRAGMA busy_timeout = 5000")
        # The default cache size is 2MB but we can afford more! Note negative
        # values set cache size in KB, positive numbers set it by number of
        # database pages
        conn.execute("PRAGMA cache_size = -256000")

    return cache[filename]


class MigrationNeeded(Exception):
    pass


def db_status(filename):
    # this allows us to use per-test :memory: dbs
    if isinstance(filename, str):
        if filename.startswith("file:") and "mode=memory" in filename:
            return ("memory", False)

        filename = Path(filename)

    if filename.exists():
        return ("file", True)

    return ("file", False)


def ensure_valid_db(filename=None, migrations=MIGRATIONS):
    # we store migrations in models, so make sure this has been imported to collect them
    import controller.models  # noqa: F401

    filename = filename_or_get_default(filename)

    db_type, db_exists = db_status(filename)
    if db_type == "file" and not db_exists:
        raise MigrationNeeded(
            f"db {filename} does not exist. Create with migrate command"
        )

    latest_version = max(migrations, default=0)
    conn = get_connection(filename)
    current_version = conn.execute("PRAGMA user_version").fetchone()[0]
    if latest_version != current_version:
        raise MigrationNeeded(
            f"db {filename} is out of date. Update with migrate command"
        )


def ensure_db(filename=None, migrations=MIGRATIONS, verbose=False):
    """Ensure db is created and up to date with migrations

    Will create new tables, or migrate the exisiting ones as needed.
    """
    # we store migrations in models, so make sure this has been imported to collect them
    import controller.models  # noqa: F401

    filename = filename_or_get_default(filename)

    db_type, db_exists = db_status(filename)

    if db_type == "file":
        filename.parent.mkdir(exist_ok=True, parents=True)

    conn = get_connection(filename)

    if db_exists:
        migrate_db(conn, migrations, verbose=verbose)
    else:  # new db
        for table in TABLES.values():
            create_table(conn, table)
        # set migration level to highest migration version
        conn.execute(f"PRAGMA user_version={max(migrations, default=0)}")
        if verbose:
            log.info(f"created new db at {filename}")
    return conn


def create_table(conn, cls):
    conn.executescript(cls.__tableschema__)


# ensure migration is applied as a transaction together with the pragma update
MIGRATION_SQL = """
BEGIN;
{sql};
PRAGMA user_version={version};
COMMIT;
"""


def migrate_db(conn, migrations=None, verbose=False):
    current_version = conn.execute("PRAGMA user_version").fetchone()[0]
    applied = []

    for version, sql in sorted(migrations.items()):
        if version > current_version:
            transaction_sql = MIGRATION_SQL.format(sql=sql, version=version)
            conn.executescript(transaction_sql)
            applied.append(version)
            if verbose:
                log.info(f"Applied migration {version}:\n{sql}")
        else:
            if verbose:
                log.info(f"Skipping migration {version} as already applied")

    return applied


def query_params_to_sql(params):
    """
    Turn a dict of query parameters into a pair of (SQL string, SQL values). All
    parameters are implicitly ANDed together, and there's a bit of magic to handle
    `field__in=list_of_values` queries, GLOB queries and Enum classes.
    """
    if not params:
        return "1 = 1", []

    parts = []
    values = []

    for key, value in params.items():
        if key.endswith("__in"):
            field = key[:-4]
            placeholders = ", ".join(["?"] * len(value))
            parts.append(f"{escape(field)} IN ({placeholders})")
            values.extend(value)
        elif key.endswith("__glob"):
            field = key[:-6]
            parts.append(f"{escape(field)} GLOB ?")
            values.append(value)
        elif key.endswith("__lt"):
            field = key[:-4]
            parts.append(f"{escape(field)} < ?")
            values.append(value)
        elif key.endswith("__gt"):
            field = key[:-4]
            parts.append(f"{escape(field)} > ?")
            values.append(value)
        elif value is None:
            parts.append(f"{escape(key)} is NULL")
        else:
            parts.append(f"{escape(key)} = ?")
            values.append(value)

    # Bit of a hack: convert any Enum instances to their values so we can use
    # them in querying
    values = [v.value if isinstance(v, Enum) else v for v in values]

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


def fast_decode_field_values(fields, row, itemclass):
    """
    Takes a list of dataclass fields and a SQLite row (or any dict-like) and
    returns field values as a list with the appropriate conversions applied
    """
    # values = []
    # breakpoint()
    # for field in fields:
    #     # print(f"{field.name} - {field.type}")
    #     value = row[field.name]
    #     # # Dicts and lists get decoded from JSON
    #     if field.type in (list, dict) and value is not None:
    #         value = json.loads(value)
    #     # # Enums get transformed back from their string/int values
    #     elif issubclass(field.type, Enum) and value is not None:
    #         value = field.type(value)
    #     # Bools get converted from int to True/False
    #     # None values are not converted to False, as None may be semantically different to False
    #     elif field.type is bool and value is not None:
    #         value = field.type(value)
    #     values.append(value)
    # breakpoint()

    return itemclass(
        id=row["id"],
        job_request_id=row["job_request_id"],  # str
        state=fields[2].type(row["state"]),  # - <enum 'State'>
        repo_url=row["repo_url"],  # str
        commit=row["commit"],  # str
        workspace=row["workspace"],  # str
        database_name=row["database_name"],  # str
        action=row["action"],  # str
        action_repo_url=row["action_repo_url"],  # str
        action_commit=row["action_commit"],  # str
        requires_outputs_from=(row["requires_outputs_from"])
        if row["requires_outputs_from"] is not None
        else None,  # list
        wait_for_job_ids=(row["wait_for_job_ids"])
        if row["wait_for_job_ids"] is not None
        else None,  # list
        run_command=row["run_command"],  # str
        image_id=row["image_id"],  # str
        output_spec=(row["output_spec"])
        if row["output_spec"] is not None
        else None,  # dict
        outputs=(row["outputs"]) if row["outputs"] is not None else None,  # dict
        unmatched_outputs=(row["unmatched_outputs"])
        if row["unmatched_outputs"] is not None
        else None,  # list
        status_message=row["status_message"],  # str
        status_code=fields[18].type(row["status_code"])
        if row["status_code"] is not None
        else None,  # - <enum 'StatusCode'>
        cancelled=bool(row["cancelled"]),  # bool
        created_at=row["created_at"],  # int
        updated_at=row["updated_at"],  # int
        started_at=row["started_at"],  # int
        completed_at=row["completed_at"],  # int
        status_code_updated_at=row["status_code_updated_at"],  # int
        trace_context=(row["trace_context"])
        if row["trace_context"] is not None
        else None,  # dict
        level4_excluded_files=(row["level4_excluded_files"])
        if row["level4_excluded_files"] is not None
        else None,  # dict
        requires_db=bool(row["requires_db"]),  # bool
        backend=row["backend"],  # str
    )
    # return values


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
        # Bools get converted from int to True/False
        # None values are not converted to False, as None may be semantically different to False
        elif field.type is bool and value is not None:
            value = field.type(value)
        values.append(value)
    return values


def is_database_locked_error(exc):
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc)
