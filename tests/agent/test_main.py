from jobrunner import config
from jobrunner.agent import main
from tests.factories import job_definition_factory


def test_inject_db_secrets_dummy_db(monkeypatch, db):
    definition = job_definition_factory()
    definition.allow_database_access = True
    definition.database_name = "FULL"

    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", True)
    monkeypatch.setattr(config, "DATABASE_URLS", {"FULL": "dburl"})

    main.inject_db_secrets(definition)

    assert "DATABASE_URL" not in definition.env


def test_inject_db_secrets(monkeypatch, db):
    definition = job_definition_factory()
    definition.allow_database_access = True
    definition.database_name = "FULL"

    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    monkeypatch.setattr(config, "DATABASE_URLS", {"FULL": "dburl"})
    monkeypatch.setattr(config, "TEMP_DATABASE_NAME", "tempdb")
    monkeypatch.setattr(config, "PRESTO_TLS_KEY", "key")
    monkeypatch.setattr(config, "PRESTO_TLS_CERT", "cert")
    monkeypatch.setattr(config, "EMIS_ORGANISATION_HASH", "hash")

    main.inject_db_secrets(definition)

    assert definition.env["DATABASE_URL"] == "dburl"
    assert definition.env["TEMP_DATABASE_NAME"] == "tempdb"
    assert definition.env["PRESTO_TLS_KEY"] == "key"
    assert definition.env["PRESTO_TLS_CERT"] == "cert"
    assert definition.env["EMIS_ORGANISATION_HASH"] == "hash"


def test_inject_db_secrets_none_configured(monkeypatch, db):
    definition = job_definition_factory()
    definition.allow_database_access = True
    definition.database_name = "FULL"

    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    monkeypatch.setattr(config, "DATABASE_URLS", {"FULL": "dburl"})
    monkeypatch.setattr(config, "TEMP_DATABASE_NAME", None)
    monkeypatch.setattr(config, "PRESTO_TLS_KEY", None)
    monkeypatch.setattr(config, "PRESTO_TLS_CERT", None)
    monkeypatch.setattr(config, "EMIS_ORGANISATION_HASH", None)

    main.inject_db_secrets(definition)

    assert definition.env["DATABASE_URL"] == "dburl"
    assert "TEMP_DATABASE_NAME" not in definition.env
    assert "PRESTO_TLS_KEY" not in definition.env
    assert "PRESTO_TLS_CERT" not in definition.env
    assert "EMIS_ORGANISATION_HASH" not in definition.env
