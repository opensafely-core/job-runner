import json
import time

import pytest
from django.urls import reverse

from controller.lib.database import find_one
from controller.models import Job, State, StatusCode, timestamp_to_isoformat
from controller.queries import set_flag
from controller.webapp.views.rap_views import job_to_api_format
from tests.conftest import get_trace
from tests.factories import job_factory, rap_api_v1_factory_raw, runjob_db_task_factory


# use a fixed time for these tests
TEST_TIME = time.time()
TEST_DATESTR = timestamp_to_isoformat(TEST_TIME)


def setup_auto_tracing():
    from opentelemetry.instrumentation.auto_instrumentation import (  # noqa: F401
        sitecustomize,
    )


@pytest.mark.parametrize(
    "flags_to_set, expected_backend_response",
    [
        # case 1: last seen set for backend
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
            ],
            [
                {
                    "name": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": None},
                    "db_maintenance": {
                        "status": "off",
                        "since": None,
                        "type": None,
                    },
                },
            ],
        ),
        # case 2: backend paused
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", "true", "test_backend"),
            ],
            [
                {
                    "name": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "on", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "off",
                        "since": None,
                        "type": None,
                    },
                },
            ],
        ),
        # case 3: backend not paused
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", None, "test_backend"),
            ],
            [
                {
                    "name": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "off",
                        "since": None,
                        "type": None,
                    },
                },
            ],
        ),
        # case 4: backend in scheduled db maintenance
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", None, "test_backend"),
                ("mode", "db-maintenance", "test_backend"),
            ],
            [
                {
                    "name": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "on",
                        "since": TEST_DATESTR,
                        "type": "scheduled",
                    },
                },
            ],
        ),
        # case 5: backend in manual db maintenance
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", None, "test_backend"),
                ("mode", "db-maintenance", "test_backend"),
                ("manual-db-maintenance", "on", "test_backend"),
            ],
            [
                {
                    "name": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "on",
                        "since": TEST_DATESTR,
                        "type": "manual",
                    },
                },
            ],
        ),
        # case 6: backend has previously been in db maintenance
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", None, "test_backend"),
                ("mode", None, "test_backend"),
                ("manual-db-maintenance", None, "test_backend"),
            ],
            [
                {
                    "name": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "off",
                        "since": TEST_DATESTR,
                        "type": None,
                    },
                },
            ],
        ),
    ],
)
def test_backends_status_view(
    db, client, monkeypatch, freezer, flags_to_set, expected_backend_response
):
    freezer.move_to(TEST_DATESTR)
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS",
        {"test_token": ["test_backend"]},
    )
    headers = {"Authorization": "test_token"}

    # set flag for unauthorised backend
    set_flag("foo", "bar", "test_other_backend")
    # set flag for authorised backends
    for flag_id, value, backend in flags_to_set:
        set_flag(flag_id, value, backend)

    response = client.get(reverse("backends_status"), headers=headers)
    assert response.status_code == 200
    response_json = response.json()

    assert response_json["backends"] == expected_backend_response


def test_backends_status_view_no_flags(db, client, monkeypatch):
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS", {"test_token": ["test", "foo"]}
    )
    headers = {"Authorization": "test_token"}

    response = client.get(reverse("backends_status"), headers=headers)
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["backends"] == [
        {
            "name": "test",
            "last_seen": None,
            "paused": {"status": "off", "since": None},
            "db_maintenance": {
                "status": "off",
                "since": None,
                "type": None,
            },
        },
        {
            "name": "foo",
            "last_seen": None,
            "paused": {"status": "off", "since": None},
            "db_maintenance": {
                "status": "off",
                "since": None,
                "type": None,
            },
        },
    ]


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


@pytest.mark.parametrize("agent_results", [True, False])
def test_job_to_api_format_metrics(db, agent_results):
    job = job_factory(state=State.RUNNING, action="action1", backend="test")
    if agent_results:
        runjob_db_task_factory(
            job=job,
            agent_results={
                "job_metrics": {"test": 0.0},
            },
        )
    else:
        runjob_db_task_factory(
            job=job,
        )

    if agent_results:
        assert job_to_api_format(job)["metrics"]["test"] == 0.0
    else:
        assert job_to_api_format(job)["metrics"] == {}


@pytest.mark.parametrize(
    "state, status_code",
    [
        (State.PENDING, StatusCode.CREATED),
        (State.RUNNING, StatusCode.EXECUTING),
        (State.SUCCEEDED, StatusCode.SUCCEEDED),
        (State.FAILED, StatusCode.DEPENDENCY_FAILED),
    ],
)
def test_status_view(db, client, monkeypatch, state, status_code):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(
        state=state, status_code=status_code, action="action1", backend="test"
    )

    post_data = {"rap_ids": [job.job_request_id]}
    response = client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )

    assert response.status_code == 200

    expected_started_at = None
    if state not in [State.PENDING]:
        expected_started_at = job.started_at_isoformat
    assert response.json() == {
        "jobs": [
            {
                "action": "action1",
                "backend": "test",
                "completed_at": None,
                "created_at": job.created_at_isoformat,
                "identifier": job.id,
                "metrics": {},
                "rap_id": job.job_request_id,
                "requires_db": False,
                "run_command": "python myscript.py",
                "started_at": expected_started_at,
                "status": state.value,
                # TODO: weird that this doesn't change - test data issue?
                "status_code": status_code.value,
                "status_message": "",
                "trace_context": job.trace_context,
                "updated_at": job.updated_at_isoformat,
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
