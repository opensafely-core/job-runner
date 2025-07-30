import time

from django.urls import reverse

from controller.models import timestamp_to_isoformat
from controller.queries import set_flag
from tests.conftest import get_trace


# use a fixed time for these tests
TEST_TIME = time.time()
TEST_DATESTR = timestamp_to_isoformat(TEST_TIME)


def setup_auto_tracing():
    from opentelemetry.instrumentation.auto_instrumentation import (  # noqa: F401
        sitecustomize,
    )


def test_backend_status_view(db, client, monkeypatch, freezer):
    freezer.move_to(TEST_DATESTR)
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    # set flag for different backend
    set_flag("foo", "bar", "test_backend")
    # set flag for this backend
    set_flag("foo", "bar1", "test")
    set_flag("pause", "true", "test")

    response = client.get(reverse("backend_status", args=("test",)), headers=headers)
    response = response.json()
    assert response["flags"] == {
        "foo": {"v": "bar1", "ts": TEST_DATESTR},
        "pause": {"v": "true", "ts": TEST_DATESTR},
    }


def test_backend_status_view_no_flags(db, client, monkeypatch):
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS", {"test_token": ["test", "foo"]}
    )
    headers = {"Authorization": "test_token"}

    response = client.get(reverse("backend_status", args=("test",)), headers=headers)
    response = response.json()
    assert response["flags"] == {}


def test_backend_status_view_tracing(db, client, monkeypatch):
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS", {"test_token": ["test", "foo"]}
    )
    headers = {"Authorization": "test_token"}
    setup_auto_tracing()
    client.get(reverse("backend_status", args=("test",)), headers=headers)

    traces = get_trace()
    last_trace = traces[-1]
    # default django attributes
    assert last_trace.attributes["http.method"] == "GET"
    assert last_trace.attributes["http.url"].endswith("/backend/status/")
    assert last_trace.attributes["http.status_code"] == 200
    # custom attributes
    assert last_trace.attributes["backend"] == "test"


def test_backend_status_no_token(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    response = client.get(reverse("backend_status", args=("test",)))
    assert response.status_code == 401
    response_json = response.json()
    assert response_json == {"error": "Unauthorized", "details": "No token provided"}


def test_backend_status_invalid_backend(db, client, monkeypatch, freezer):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}
    response = client.get(reverse("backend_status", args=("foo",)), headers=headers)
    assert response.status_code == 404
    response_json = response.json()
    assert response_json == {"error": "Not found", "details": "Backend 'foo' not found"}


def test_backend_status_invalid_backend_for_token(db, client, monkeypatch, freezer):
    monkeypatch.setattr("common.config.BACKENDS", ["test", "foo"])
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}
    response = client.get(reverse("backend_status", args=("foo",)), headers=headers)
    assert response.status_code == 401
    response_json = response.json()
    assert response_json == {
        "error": "Unauthorized",
        "details": "Invalid token for backend 'foo'",
    }


def test_backend_status_invalid_token(db, client, monkeypatch, freezer):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "unknown_token"}
    response = client.get(reverse("backend_status", args=("test",)), headers=headers)
    assert response.status_code == 401
    response_json = response.json()
    assert response_json == {"error": "Unauthorized", "details": "Invalid token"}


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
    response = response.json()
    assert response["flags"] == {
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
    response = response.json()
    assert response["flags"] == {"test": {}, "foo": {}}


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
    assert last_trace.attributes["http.method"] == "GET"
    assert last_trace.attributes["http.url"].endswith("/backend/status/")
    assert last_trace.attributes["http.status_code"] == 200


def test_backends_status_no_token(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    response = client.get(reverse("backends_status"))
    assert response.status_code == 401
    response_json = response.json()
    assert response_json == {"error": "Unauthorized", "details": "No token provided"}


def test_backends_status_invalid_token(db, client, monkeypatch, freezer):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "unknown_token"}
    response = client.get(reverse("backends_status"), headers=headers)
    assert response.status_code == 401
    response_json = response.json()
    assert response_json == {"error": "Unauthorized", "details": "Invalid token"}
