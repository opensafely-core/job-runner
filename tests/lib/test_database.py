import logging
import sqlite3

import pytest

from jobrunner.lib.database import (
    CONNECTION_CACHE,
    MigrationNeeded,
    count_where,
    ensure_db,
    ensure_valid_db,
    exists_where,
    find_one,
    generate_insert_sql,
    get_connection,
    insert,
    migrate_db,
    query_params_to_sql,
    select_values,
    transaction,
    update,
    upsert,
)
from jobrunner.models import Job, State


def test_get_connection():
    db = "file:test_get_connection?mode=memory&cache=shared"
    conn = get_connection(db)
    assert conn is get_connection(db)


def test_basic_roundtrip(tmp_work_dir):
    job = Job(
        id="foo123",
        job_request_id="bar123",
        state=State.RUNNING,
        output_spec={"hello": [1, 2, 3]},
    )
    insert(job)
    j = find_one(Job, job_request_id__in=["bar123", "baz123"])
    assert job.id == j.id
    assert job.output_spec == j.output_spec


def test_insert_in_transaction_success(tmp_work_dir):
    job = Job(
        id="foo123",
        job_request_id="bar123",
        state=State.RUNNING,
        output_spec={"hello": [1, 2, 3]},
    )

    with transaction():
        insert(job)
    j = find_one(Job, job_request_id__in=["bar123", "baz123"])
    assert job.id == j.id
    assert job.output_spec == j.output_spec


def test_insert_in_transaction_fail(tmp_work_dir):
    job = Job(
        id="foo123",
        job_request_id="bar123",
        state=State.RUNNING,
        output_spec={"hello": [1, 2, 3]},
    )

    with transaction():
        insert(job)
        conn = get_connection()
        conn.execute("ROLLBACK")

    with pytest.raises(ValueError):
        find_one(Job, job_request_id__in=["bar123", "baz123"])


def test_generate_insert_sql(tmp_work_dir):
    job = Job(id="foo123", action="foo")
    sql, _ = generate_insert_sql(job)

    assert (
        sql
        == 'INSERT INTO "job" ("id", "job_request_id", "state", "repo_url", "commit", "workspace", "database_name", "action", "action_repo_url", "action_commit", "requires_outputs_from", "wait_for_job_ids", "run_command", "image_id", "output_spec", "outputs", "unmatched_outputs", "status_message", "status_code", "cancelled", "created_at", "updated_at", "started_at", "completed_at", "status_code_updated_at", "trace_context", "level4_excluded_files", "requires_db") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    )


def test_update(tmp_work_dir):
    job = Job(id="foo123", action="foo")
    insert(job)
    job.action = "bar"
    update(job)
    assert find_one(Job, id="foo123").action == "bar"


def test_upsert_insert(tmp_work_dir):
    job = Job(id="foo123", action="bar")
    upsert(job)
    assert find_one(Job, id="foo123").action == "bar"


def test_upsert_update(tmp_work_dir):
    job = Job(id="foo123", action="foo")
    insert(job)
    job.action = "bar"
    upsert(job)
    assert find_one(Job, id="foo123").action == "bar"


def test_update_excluding_a_field(tmp_work_dir):
    job = Job(id="foo123", action="foo", commit="commit-of-glory")
    insert(job)
    job.action = "bar"
    job.commit = "commit-of-doom"
    update(job, exclude_fields=["commit"])
    j = find_one(Job, id="foo123")
    assert j.action == "bar"
    assert j.commit == "commit-of-glory"


def test_exists_where(tmp_work_dir):
    insert(Job(id="foo123", state=State.PENDING))
    insert(Job(id="foo124", state=State.RUNNING))
    insert(Job(id="foo125", state=State.FAILED))
    job_state_exists = exists_where(Job, state__in=[State.PENDING, State.FAILED])
    assert job_state_exists is True
    job_id_exists = exists_where(Job, id="foo124")
    assert job_id_exists is True


def test_count_where(tmp_work_dir):
    insert(Job(id="foo123", state=State.PENDING))
    insert(Job(id="foo124", state=State.RUNNING))
    insert(Job(id="foo125", state=State.FAILED))
    jobs_in_states = count_where(Job, state__in=[State.PENDING, State.FAILED])
    assert jobs_in_states == 2
    jobs_with_id = count_where(Job, id="foo124")
    assert jobs_with_id == 1


def test_select_values(tmp_work_dir):
    insert(Job(id="foo123", state=State.PENDING))
    insert(Job(id="foo124", state=State.RUNNING))
    insert(Job(id="foo125", state=State.FAILED))
    values = select_values(Job, "id", state__in=[State.PENDING, State.FAILED])
    assert sorted(values) == ["foo123", "foo125"]
    values = select_values(Job, "state", id="foo124")
    assert values == [State.RUNNING]


def test_find_one_returns_a_single_value(tmp_work_dir):
    insert(Job(id="foo123", workspace="the-workspace"))
    job = find_one(Job, id="foo123")
    assert job.workspace == "the-workspace"


def test_find_one_fails_if_there_are_no_results(tmp_work_dir):
    with pytest.raises(ValueError):
        find_one(Job, id="foo123")


def test_find_one_fails_if_there_is_more_than_one_result(tmp_work_dir):
    insert(Job(id="foo123", workspace="the-workspace"))
    insert(Job(id="foo456", workspace="the-workspace"))
    with pytest.raises(ValueError):
        find_one(Job, workspace="the-workspace")


def test_ensure_db_new_db(tmp_path):
    db = tmp_path / "db.sqlite"

    conn = ensure_db(db, {1: "should not run"})

    assert conn.isolation_level is None
    assert conn.row_factory is sqlite3.Row
    assert CONNECTION_CACHE.__dict__[db] is conn
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_ensure_db_verbose(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    db = tmp_path / "db.sqlite"

    ensure_db(db, {1: "should not run"}, verbose=True)
    assert "created new db" in caplog.records[-1].msg

    ensure_db(
        db,
        {1: "should not run", 2: "ALTER TABLE job ADD COLUM test TEXT"},
        verbose=True,
    )
    assert "Skipping migration 1" in caplog.records[-2].msg
    assert "Applied migration 2" in caplog.records[-1].msg


def test_ensure_db_new_db_memory():
    db = "file:test?mode=memory&cached=shared"
    conn = ensure_db(db, {1: "should not run"})
    assert CONNECTION_CACHE.__dict__[db] is conn
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_ensure_db_existing_db_needs_migration(tmp_path):
    db = tmp_path / "db.sqlite"
    conn = ensure_db(db, {})
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    CONNECTION_CACHE.__dict__.clear()

    ensure_db(db, {1: "ALTER TABLE job ADD COLUMN test TEXT"})

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_ensure_db_existing_db_does_not_need_migration(tmp_path):
    db = tmp_path / "db.sqlite"
    conn = ensure_db(db, {})
    conn.execute("PRAGMA user_version=1")
    assert CONNECTION_CACHE.__dict__.pop(db)

    # shouldn't run this
    conn = ensure_db(db, {1: "should not run"})
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_migrate_db(tmp_path):
    db = tmp_path / "db.sqlite"
    conn = ensure_db(db, {})
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0

    migrations = {
        1: "ALTER TABLE job ADD COLUMN first TEXT;",
    }

    applied = migrate_db(conn, migrations)
    assert applied == [1]
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1

    # this will error if first column doesn't exist
    conn.execute("SELECT first FROM job")

    migrations[2] = "ALTER TABLE job ADD COLUMN second TEXT;"

    applied = migrate_db(conn, migrations)
    assert applied == [2]
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
    conn.execute("SELECT second FROM job")

    applied = migrate_db(conn, migrations)
    assert applied == []
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 2


def test_migrate_in_transaction(tmp_path):
    db = tmp_path / "db.sqlite"
    conn = ensure_db(db, {})

    migrations = {
        1: "bad migration",
    }

    version = conn.execute("PRAGMA user_version").fetchone()[0]

    with pytest.raises(Exception):
        migrate_db(conn, migrations)

    # check version not incremented
    assert version == conn.execute("PRAGMA user_version").fetchone()[0]


def test_ensure_valid_db(tmp_path):
    # db doesn't exists
    with pytest.raises(MigrationNeeded) as exc:
        ensure_valid_db("not_exists")

    assert "does not exist" in str(exc.value)

    # db exists but is out of date
    db = tmp_path / "db.sqlite"
    conn = ensure_db(db, {})

    with pytest.raises(MigrationNeeded) as exc:
        ensure_valid_db(db, {1: "should not run"})

    assert "out of date" in str(exc.value)

    # does not raise when all is well
    conn.execute("PRAGMA user_version=1")
    ensure_valid_db(db, {1: "should not run"})


@pytest.mark.parametrize(
    "params,expected_sql_string,expected_sql_values",
    [
        ({}, "1 = 1", []),
        ({"doubutsu": "neko"}, '"doubutsu" = ?', ["neko"]),
        ({"doubutsu__like": "ne%"}, '"doubutsu" LIKE ?', ["ne%"]),
        (
            {"doubutsu__in": ["neko", "kitsune", "nezumi"]},
            '"doubutsu" IN (?, ?, ?)',
            ["neko", "kitsune", "nezumi"],
        ),
        (
            {"namae": "rosa", "doubutsu__in": ["neko"]},
            '"namae" = ? AND "doubutsu" IN (?)',
            ["rosa", "neko"],
        ),
        ({"state": State.RUNNING}, '"state" = ?', ["running"]),
    ],
)
def test_query_params_to_sql(params, expected_sql_string, expected_sql_values):
    sql_string, sql_values = query_params_to_sql(params)
    assert sql_string == expected_sql_string
    assert sql_values == expected_sql_values
