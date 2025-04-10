from jobrunner import config
from jobrunner.agent import main
from tests.factories import job_definition_factory


def test_inject_db_secrets_dummy_db(monkeypatch, db):
    defintion = job_definition_factory()
    defintion.allow_database_access = True
    defintion.database_name = "FULL"

    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", True)
    monkeypatch.setattr(config, "DATABASE_URLS", {"FULL": "dburl"})

    main.inject_db_secrets(defintion)

    assert "DATABASE_URL" not in defintion.env


def test_inject_db_secrets(monkeypatch, db):
    defintion = job_definition_factory()
    defintion.allow_database_access = True
    defintion.database_name = "FULL"

    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    monkeypatch.setattr(config, "DATABASE_URLS", {"FULL": "dburl"})
    monkeypatch.setattr(config, "TEMP_DATABASE_NAME", "tempdb")
    monkeypatch.setattr(config, "PRESTO_TLS_KEY", "key")
    monkeypatch.setattr(config, "PRESTO_TLS_CERT", "cert")
    monkeypatch.setattr(config, "EMIS_ORGANISATION_HASH", "hash")

    main.inject_db_secrets(defintion)

    assert defintion.env["DATABASE_URL"] == "dburl"
    assert defintion.env["TEMP_DATABASE_NAME"] == "tempdb"
    assert defintion.env["PRESTO_TLS_KEY"] == "key"
    assert defintion.env["PRESTO_TLS_CERT"] == "cert"
    assert defintion.env["EMIS_ORGANISATION_HASH"] == "hash"
