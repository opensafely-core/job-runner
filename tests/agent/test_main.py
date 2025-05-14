from unittest.mock import ANY, patch

import pytest

from jobrunner.agent import main
from jobrunner.config import agent as config
from jobrunner.job_executor import ExecutorState, JobStatus
from tests.factories import job_definition_factory, runjob_db_task_factory


def test_inject_db_secrets_dummy_db(monkeypatch, db):
    definition = job_definition_factory(requires_db=True, database_name="FULL")

    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", True)
    monkeypatch.setattr(config, "DATABASE_URLS", {"FULL": "dburl"})

    main.inject_db_secrets(definition)

    assert "DATABASE_URL" not in definition.env


def test_inject_db_secrets(monkeypatch, db):
    definition = job_definition_factory(requires_db=True, database_name="FULL")

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
    definition = job_definition_factory(requires_db=True, database_name="FULL")

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


def test_inject_db_secrets_invalid_db_name(monkeypatch, db):
    definition = job_definition_factory(database_name="foo", requires_db=True)

    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    monkeypatch.setattr(config, "DATABASE_URLS", {"FULL": "dburl"})

    with pytest.raises(
        ValueError, match="Database name 'foo' is not currently defined"
    ):
        main.inject_db_secrets(definition)


@patch("jobrunner.agent.tracing.set_job_results_metadata")
@patch("jobrunner.agent.task_api.update_controller")
@pytest.mark.parametrize(
    "output_results,expected_redacted_results",
    [
        (
            dict(
                unmatched_outputs=["output/foo.txt"],
                unmatched_patterns=["outputs/foo_*.txt"],
                level4_excluded_files=[],
                status_message="An unmatched output",
                hint="An unmatched pattern",
            ),
            dict(
                exit_code=0,
                has_unmatched_patterns=True,
                has_level4_excluded_files=False,
                status_message="",
                hint="",
            ),
        ),
        (
            dict(
                unmatched_outputs=[],
                unmatched_patterns=[],
                level4_excluded_files=[],
                status_message="Complete",
                hint="nothing to see here",
            ),
            dict(
                exit_code=0,
                has_unmatched_patterns=False,
                has_level4_excluded_files=False,
                status_message="Complete",
                hint="nothing to see here",
            ),
        ),
        (
            dict(
                unmatched_outputs=[],
                unmatched_patterns=[],
                level4_excluded_files=["output/foo.txt"],
                status_message="Complete",
                hint="nothing to see here",
            ),
            dict(
                exit_code=0,
                has_unmatched_patterns=False,
                has_level4_excluded_files=True,
                status_message="Complete",
                hint="nothing to see here",
            ),
        ),
    ],
)
def test_update_job_task_results_redacted(
    mock_update_controller,
    mock_set_job_results_metadata,
    db,
    output_results,
    expected_redacted_results,
):
    task = runjob_db_task_factory()
    job_results = dict(
        exit_code=0,
        outputs={"output/foo.txt": "moderately_sensitive"},
    )
    job_results.update(output_results)

    job_status = JobStatus(ExecutorState.FINALIZED, results=job_results)
    previous_job_status = JobStatus(ExecutorState.EXECUTED, results={})

    main.update_job_task(task, job_status, previous_job_status, complete=True)

    mock_update_controller.assert_called_with(
        task,
        job_status.state.value,
        expected_redacted_results,
        True,
    )
    mock_set_job_results_metadata.assert_called_with(
        ANY,
        expected_redacted_results,
        {
            "final_job_status": job_status.state.name,
            "complete": True,
        },
    )
