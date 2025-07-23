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
    monkeypatch.setattr("controller.config.JOB_SERVER_TOKENS", {"test": "test_token"})
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
    monkeypatch.setattr("controller.config.JOB_SERVER_TOKENS", {"test": "test_token"})
    headers = {"Authorization": "test_token"}

    response = client.get(reverse("backend_status", args=("test",)), headers=headers)
    response = response.json()
    assert response["flags"] == {}


def test_backend_status_view_tracing(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.JOB_SERVER_TOKENS", {"test": "test_token"})
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
