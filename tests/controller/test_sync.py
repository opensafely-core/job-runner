import pytest
from responses import matchers

from controller import config, sync
from controller.lib.database import find_where
from controller.models import Job, JobRequest
from tests.conftest import get_trace


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


def test_session_request(db, responses):
    responses.add(
        method="GET",
        url=f"{config.JOB_SERVER_ENDPOINT}path/",
        status=200,
        json="{}",
        match=[
            matchers.header_matcher(
                {
                    "Authorization": config.JOB_SERVER_TOKENS["test"],
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
    ]:
        assert attribute in span.attributes
