import json
import time

from django.urls import reverse

from controller.lib.database import find_one
from controller.models import Job, State, timestamp_to_isoformat
from controller.queries import set_flag
from tests.conftest import get_trace
from tests.factories import job_factory, rap_api_v1_factory_raw


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
    assert last_trace.attributes["http.route"] == ("controller/v1/backend/status/")
    assert last_trace.attributes["http.response.status_code"] == 200


def test_cancel_view(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1", backend="test")
    assert not job.cancelled
    post_data = {
        "rap_id": job.job_request_id,
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
        "details": "Invalid request body received: 'rap_id' is a required property",
    }


def test_cancel_view_no_jobs_for_rap_id(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    post_data = {
        "rap_id": "abcdefgh12345678",
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
        "error": "jobs not found",
        "details": "No jobs found for rap_id abcdefgh12345678",
        "rap_id": "abcdefgh12345678",
    }


def test_cancel_view_actions_not_found(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1", backend="test")

    post_data = {
        "rap_id": job.job_request_id,
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
        "rap_id": job.job_request_id,
        "not_found": ["action2", "action3"],
    }


def test_cancel_view_not_allowed_for_backend(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1", backend="foo")

    post_data = {
        "rap_id": job.job_request_id,
        "actions": ["action1"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 403
    response_json = response.json()
    assert response_json == {
        "error": "Not allowed",
        "details": "Not allowed for backend 'foo'",
    }


def test_create_view(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    rap_request_body = rap_api_v1_factory_raw()

    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        "success": "ok",
        "details": f"Received job request {rap_request_body['rap_id']}",
        "rap_id": rap_request_body["rap_id"],
    }, response
    # TODO: uncomment when create view actually creates jobs
    # job = find_one(Job, job_request_id=rap_request_body["rap_id"])
    # assert job.action == "action"


def test_create_view_validation_error(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    rap_request_body = rap_api_v1_factory_raw(requested_actions=[])
    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )

    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "Validation error",
        "details": "Invalid request body received at $.requested_actions: [] should be non-empty",
    }


def test_create_view_not_allowed_for_backend(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    rap_request_body = rap_api_v1_factory_raw(backend="foo")

    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 403, response.json()
    response_json = response.json()
    assert response_json == {
        "error": "Not allowed",
        "details": "Not allowed for backend 'foo'",
    }


def test_status_view(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    # parameterise with some different job status
    job = job_factory(state=State.PENDING, action="action1", backend="test")

    post_data = {"rap_ids": [job.job_request_id]}
    response = client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        "rap_statuses": [
            {
                "rap_id": job.job_request_id,
                "status": "ok",
                "details": "I'm sure it's fine",
            }
        ],
    }, response


def test_status_view_validation_error(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    post_data = {}
    response = client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "Validation error",
        "details": "Invalid request body received: 'rap_ids' is a required property",
    }


def test_status_view_no_jobs_for_rap_id(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    post_data = {"rap_ids": ["abcdefgh12345678"]}
    response = client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "jobs not found",
        "details": "No jobs found for rap_id abcdefgh12345678",
        "rap_id": "abcdefgh12345678",
    }


def test_status_view_not_allowed_for_backend(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1", backend="foo")

    post_data = {"rap_ids": [job.job_request_id]}
    response = client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 403
    response_json = response.json()
    assert response_json == {
        "error": "Not allowed",
        "details": "Not allowed for backend 'foo'",
    }
