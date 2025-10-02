import json

import pytest
from responses import matchers

from controller import config, queries, sync
from controller.lib.database import find_where
from controller.models import Job, JobRequest, State
from tests.conftest import get_trace
from tests.factories import job_factory, runjob_db_task_factory


def test_job_request_from_remote_format():
    remote_job_request = {
        "identifier": "123",
        "workspace": {
            "name": "testing",
            "repo": "https://github.com/opensafely/foo",
            "branch": "master",
        },
        "database_name": "default",
        "requested_actions": ["generate_cohort"],
        "cancelled_actions": ["analyse"],
        "force_run_dependencies": True,
        "sha": "abcdef",
        "codelists_ok": True,
        "created_by": "user",
        "project": "project",
        "orgs": ["org"],
        "backend": "test",
    }
    expected = JobRequest(
        id="123",
        repo_url="https://github.com/opensafely/foo",
        commit="abcdef",
        branch="master",
        workspace="testing",
        codelists_ok=True,
        database_name="default",
        requested_actions=["generate_cohort"],
        cancelled_actions=["analyse"],
        force_run_dependencies=True,
        backend="test",
        original=remote_job_request,
    )
    job_request = sync.job_request_from_remote_format(remote_job_request)
    assert job_request == expected


def test_job_to_remote_format_default(db):
    job = job_factory()

    json = sync.job_to_remote_format(job)

    assert json["action"] == "action_name"
    assert json["run_command"] == "python myscript.py"
    assert json["status"] == "pending"
    assert json["status_code"] == "created"
    assert json["metrics"] == {}
    assert json["requires_db"] is False


def test_job_to_remote_format_null_status_message(db):
    job = job_factory(status_message=None)
    json = sync.job_to_remote_format(job)
    assert json["status_message"] == ""


def test_job_to_remote_format_metrics(db):
    job = job_factory(state=State.RUNNING)
    runjob_db_task_factory(
        job=job,
        agent_results={
            "job_metrics": {"test": 0.0},
        },
    )
    json = sync.job_to_remote_format(job)

    assert json["metrics"] == {"test": 0.0}


def test_session_request_no_flags(db, responses):
    responses.add(
        method="GET",
        url=f"{config.JOB_SERVER_ENDPOINT}path/",
        status=200,
        json="{}",
        match=[
            matchers.header_matcher(
                {
                    "Authorization": config.JOB_SERVER_TOKENS["test"],
                    "Flags": "{}",
                }
            ),
        ],
    )

    # if this works, our expected request was generated
    sync.api_get("path", backend="test")


def test_session_request_flags(db, responses):
    f1 = queries.set_flag("mode", "db-maintenance", backend="test")
    f2 = queries.set_flag("pause", "true", backend="test")

    flags_dict = {
        "mode": {"v": "db-maintenance", "ts": f1.timestamp_isoformat},
        "pause": {"v": "true", "ts": f2.timestamp_isoformat},
    }
    expected_header = json.dumps(flags_dict, separators=(",", ":"))

    responses.add(
        method="GET",
        url=f"{config.JOB_SERVER_ENDPOINT}path/",
        status=200,
        json="{}",
        match=[
            matchers.header_matcher(
                {
                    "Authorization": config.JOB_SERVER_TOKENS["test"],
                    "Flags": expected_header,
                }
            ),
        ],
    )

    # if this works, our expected request was generated
    sync.api_get("path", backend="test")


def test_sync_empty_response(db, monkeypatch, responses):
    monkeypatch.setattr(
        "controller.config.JOB_SERVER_ENDPOINT", "http://testserver/api/v2/"
    )
    responses.add(
        method="GET",
        status=200,
        url="http://testserver/api/v2/job-requests/",
        json={
            "results": [],
        },
    )
    sync.sync()

    # verify we did not post back to job-server
    last_request = responses.calls[-1].request
    assert last_request.body is None
    assert last_request.method == "GET"

    # also that we did not create any jobs
    jobs = find_where(Job)
    assert jobs == []


def test_session_request_multiple_backends(db, monkeypatch, responses):
    monkeypatch.setattr("common.config.BACKENDS", ["foo", "bar"])
    monkeypatch.setattr(
        "controller.config.JOB_SERVER_TOKENS",
        {"foo": "token-foo", "bar": "token-bar"},
    )

    responses.add(
        method="GET",
        url=f"{config.JOB_SERVER_ENDPOINT}job-requests/",
        status=200,
        json={"results": []},
        match=[
            matchers.header_matcher(
                {
                    "Authorization": "token-foo",
                    "Flags": "{}",
                }
            ),
        ],
    )
    responses.add(
        method="GET",
        url=f"{config.JOB_SERVER_ENDPOINT}job-requests/",
        status=200,
        json={"results": []},
        match=[
            matchers.header_matcher(
                {
                    "Authorization": "token-bar",
                    "Flags": "{}",
                }
            ),
        ],
    )

    # if this passes, it means the api endpoint was called as expected for each of our
    # backends
    sync.sync()

    # empty responses, so we did not create any jobs
    jobs = find_where(Job)
    assert jobs == []


def test_sync_no_token(db, monkeypatch):
    monkeypatch.setattr("common.config.BACKENDS", ["foo"])
    monkeypatch.setattr(
        "controller.config.JOB_SERVER_ENDPOINT", "http://testserver/api/v2/"
    )
    with pytest.raises(sync.SyncAPIError, match="No api token found"):
        sync.sync()


def test_sync_telemetry(db, monkeypatch, responses, test_repo):
    # Fake endpoints to contact.
    monkeypatch.setattr("common.config.BACKENDS", ["emis"])
    monkeypatch.setattr(
        "controller.config.JOB_SERVER_TOKENS",
        {"emis": "token-foo"},
    )
    monkeypatch.setattr(
        "controller.config.JOB_SERVER_ENDPOINT", "http://testserver/api/v2/"
    )

    # Fake Job Server /job-requests API response.
    job_request = {
        "identifier": 1,
        "requested_actions": [
            "analyse_data_ehrql",
            "test_reusable_action_ehrql",
            "test_cancellation_ehrql",
        ],
        "cancelled_actions": [],
        "force_run_dependencies": False,
        "workspace": {
            "name": "testing",
            "repo": str(test_repo.path),
            "branch": "main",
        },
        "codelists_ok": True,
        "database_name": "default",
        "sha": test_repo.commit,
        "created_by": "user",
        "project": "project",
        "orgs": ["org"],
        "backend": "emis",
    }
    responses.add(
        method="GET",
        status=200,
        url="http://testserver/api/v2/job-requests/",
        json={
            "results": [job_request],
        },
    )

    # Fake Job Server /jobs API response.
    responses.add(
        method="POST",
        status=200,
        url="http://testserver/api/v2/jobs/",
        json={},
    )

    # Do the work.
    sync.sync()

    # Test there is one sync span (as one configured backend). With attributes set.
    spans = get_trace("sync")
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "sync_backend"
    assert span.attributes["backend"] == "emis"

    # Test that expected duration_ms_as_span_attr attributes are present.
    # These are in ms, rounded to the nearest int(), so in this test, they're
    # likely to be 0. Actual timing is tested in tests/common/test_tracing.py
    for attribute in [
        "api_get.duration_ms",
        "parse_requests.duration_ms",
        "find_ids.duration_ms",
        "create.duration_ms",
        "find_more_ids.duration_ms",
        "find_where.duration_ms",
        "encode_jobs.duration_ms",
        "api_post.duration_ms",
    ]:
        assert attribute in span.attributes
