import json
import time

from django.urls import reverse

from controller.lib.database import find_one
from controller.models import Job, State, timestamp_to_isoformat
from controller.queries import set_flag
from tests.conftest import get_trace
from tests.factories import job_factory


# use a fixed time for these tests
TEST_TIME = time.time()
TEST_DATESTR = timestamp_to_isoformat(TEST_TIME)


def setup_auto_tracing():
    from opentelemetry.instrumentation.auto_instrumentation import (  # noqa: F401
        sitecustomize,
    )


def test_backends_status_view(db, client, monkeypatch, freezer):
    freezer.move_to(TEST_DATESTR)
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS",
        {"test_token": ["test_backend1", "test_backend2"]},
    )
    headers = {"Authorization": "test_token"}

    # set flag for unauthorised backend
    set_flag("foo", "bar", "test_other_backend")
    # set flag for authorised backends
    set_flag("foo", "bar1", "test_backend1")
    set_flag("pause", "true", "test_backend1")
    set_flag("pause", "false", "test_backend2")

    response = client.get(reverse("backends_status"), headers=headers)
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["flags"] == {
        "test_backend1": {
            "foo": {"v": "bar1", "ts": TEST_DATESTR},
            "pause": {"v": "true", "ts": TEST_DATESTR},
        },
        "test_backend2": {
            "pause": {"v": "false", "ts": TEST_DATESTR},
        },
    }


def test_backends_status_view_no_flags(db, client, monkeypatch):
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS", {"test_token": ["test", "foo"]}
    )
    headers = {"Authorization": "test_token"}

    response = client.get(reverse("backends_status"), headers=headers)
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["flags"] == {"test": {}, "foo": {}}


def test_backends_status_view_tracing(db, client, monkeypatch):
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS", {"test_token": ["test", "foo"]}
    )
    headers = {"Authorization": "test_token"}
    setup_auto_tracing()
    client.get(reverse("backends_status"), headers=headers)

    traces = get_trace()
    last_trace = traces[-1]
    # default django attributes
    assert last_trace.attributes["http.request.method"] == "GET"
    assert last_trace.attributes["http.route"] == ("backend/status/")
    assert last_trace.attributes["http.response.status_code"] == 200


def test_cancel_view(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1")
    assert not job.cancelled
    post_data = {
        "backend": "test",
        "job_request_id": job.job_request_id,
        "actions": ["action1"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        "success": "ok",
        "details": "1 actions cancelled",
        "count": 1,
    }, response
    job = find_one(Job, id=job.id)
    assert job.cancelled


def test_cancel_view_validation_error(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    post_data = {
        "backend": "test",
        "actions": ["action1"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "Validation error",
        "details": "Invalid request body received: 'job_request_id' is a required property",
    }


def test_cancel_view_no_jobs_for_job_request_id(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    post_data = {
        "backend": "test",
        "job_request_id": "abcdefgh12345678",
        "actions": ["action1"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "job request not found",
        "details": "No jobs found for job_request_id abcdefgh12345678",
        "job_request_id": "abcdefgh12345678",
    }


def test_cancel_view_actions_not_found(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1")

    post_data = {
        "backend": "test",
        "job_request_id": job.job_request_id,
        "actions": ["action2", "action3"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "jobs not found",
        "details": "Jobs matching requested cancelled actions could not be found: action2,action3",
        "job_request_id": job.job_request_id,
        "not_found": ["action2", "action3"],
    }
