import json
import time
from datetime import datetime, timezone

from django.http import JsonResponse
from django.urls import reverse

from jobrunner.lib import database
from jobrunner.models import Task
from jobrunner.queries import get_flag_value
from tests.conftest import get_trace
from tests.factories import (
    canceljob_db_task_factory,
    runjob_db_task_factory,
)


def setup_auto_tracing():
    from opentelemetry.instrumentation.auto_instrumentation import (  # noqa: F401
        sitecustomize,
    )


def test_controller_returns_get_request_method(client):
    response = client.get("/")
    json_response = response.json()
    assert json_response["method"] == "GET"
    assert isinstance(response, JsonResponse)


def test_controller_returns_post_request_method(client):
    response = client.post("/")
    json_response = response.json()
    assert json_response["method"] == "POST"
    assert isinstance(response, JsonResponse)


def test_active_tasks_view(db, client, monkeypatch, freezer):
    mock_now = datetime(2025, 6, 1, 10, 30, tzinfo=timezone.utc)
    freezer.move_to(mock_now)

    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_TOKENS", {"test": "test_token"}
    )
    headers = {"Authorization": "test_token"}

    assert get_flag_value("last_seen_at", "test") is None

    runtask = runjob_db_task_factory(backend="test")
    response = client.get(reverse("active_tasks", args=("test",)), headers=headers)
    response = response.json()
    assert response["tasks"] == [
        {
            "id": runtask.id,
            "backend": "test",
            "type": "runjob",
            "definition": runtask.definition,
            "created_at": runtask.created_at,
            # test factory defaults
            "attributes": {
                "user": "testuser",
                "project": "project",
                "orgs": "org1,org2",
            },
        }
    ], response["tasks"][0]["attributes"]
    # Calling the tasks endpoint sets the last_seen_at flag for the backend
    assert get_flag_value("last-seen-at", "test") == mock_now.isoformat()
    mock_later = datetime(2025, 6, 1, 22, 30, tzinfo=timezone.utc)
    freezer.move_to(mock_later)
    client.get(reverse("active_tasks", args=("test",)), headers=headers)
    assert get_flag_value("last-seen-at", "test") == mock_later.isoformat()


def test_active_tasks_view_multiple_backends(db, client, monkeypatch):
    monkeypatch.setattr("jobrunner.config.common.BACKENDS", ["test", "foo"])
    monkeypatch.setattr(
        "jobrunner.config.controller.DEFAULT_JOB_CPU_COUNT", {"test": 1.0, "foo": 1.0}
    )
    monkeypatch.setattr(
        "jobrunner.config.controller.DEFAULT_JOB_MEMORY_LIMIT",
        {"test": "1G", "foo": "1G"},
    )
    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_TOKENS",
        {"test": "test_token", "foo": "foo_token"},
    )

    # active tasks on test backend
    runtask1 = runjob_db_task_factory(backend="test")
    canceltask1 = canceljob_db_task_factory(backend="test")
    # inactive tasks on test backend
    runjob_db_task_factory(backend="test", active=False)
    canceljob_db_task_factory(backend="test", active=False)
    # active tasks on other backend
    runtask2 = runjob_db_task_factory(backend="foo")
    canceltask3 = canceljob_db_task_factory(backend="foo")

    response = client.get(
        reverse("active_tasks", args=("test",)), headers={"Authorization": "test_token"}
    )
    response = response.json()

    assert {task["id"] for task in response["tasks"]} == {runtask1.id, canceltask1.id}

    response = client.get(
        reverse("active_tasks", args=("foo",)), headers={"Authorization": "foo_token"}
    )
    response = response.json()

    assert {task["id"] for task in response["tasks"]} == {runtask2.id, canceltask3.id}


def test_active_tasks_unknown_backend(db, client):
    response = client.get(reverse("active_tasks", args=("foo",)))
    assert response.status_code == 404
    assert response.json()["details"] == "Backend 'foo' not found"


def test_no_auth_token(db, client):
    response = client.get(reverse("active_tasks", args=("test",)))
    assert response.status_code == 401
    assert response.json()["details"] == "No token provided"


def test_auth_token_invalid(db, client, monkeypatch):
    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_TOKENS",
        {"test": "test_token", "foo": "foo_token"},
    )
    # headers with valid token, but for wrong backend
    response = client.get(
        reverse("active_tasks", args=("test",)), headers={"Authorization": "foo_token"}
    )
    assert response.status_code == 401
    assert response.json()["details"] == "Invalid token for backend 'test'"


def test_update_task(db, client, monkeypatch):
    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_TOKENS", {"test": "test_token"}
    )

    make_task = runjob_db_task_factory(backend="test")

    assert make_task.agent_stage is None

    post_data = {
        "task_id": make_task.id,
        "stage": "prepared",
        "results": {"foo": "bar"},
        "complete": False,
        "timestamp_ns": "",
    }

    response = client.post(
        reverse("update_task", args=("test",)),
        data={"payload": json.dumps(post_data)},
        headers={"Authorization": "test_token"},
    )

    assert response.status_code == 200

    task = database.find_one(Task, id=make_task.id)

    assert task.agent_stage == "prepared"
    assert task.agent_results == {"foo": "bar"}
    assert not task.agent_complete
    assert task.agent_timestamp_ns is None


def test_update_task_with_timestamp(db, client, monkeypatch):
    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_TOKENS", {"test": "test_token"}
    )
    make_task = runjob_db_task_factory(backend="test")

    assert make_task.agent_stage is None
    timestamp = time.time_ns()

    post_data = {
        "task_id": make_task.id,
        "stage": "prepared",
        "results": {},
        "complete": False,
        "timestamp_ns": timestamp,
    }

    response = client.post(
        reverse("update_task", args=("test",)),
        data={"payload": json.dumps(post_data)},
        headers={"Authorization": "test_token"},
    )

    assert response.status_code == 200

    task = database.find_one(Task, id=make_task.id)

    assert task.agent_stage == "prepared"
    assert task.agent_timestamp_ns == timestamp


def test_update_task_no_matching_task(db, client, monkeypatch):
    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_TOKENS", {"test": "test_token"}
    )

    post_data = {
        "task_id": "unknown-task-id",
        "stage": "prepared",
        "results": {},
        "complete": False,
        "timestamp_ns": "",
    }

    response = client.post(
        reverse("update_task", args=("test",)),
        data={"payload": json.dumps(post_data)},
        headers={"Authorization": "test_token"},
    )

    assert response.status_code == 500
    assert response.json()["error"] == "Error updating task"


def test_active_tasks_view_tracing(db, client, monkeypatch):
    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_TOKENS", {"test": "test_token"}
    )
    headers = {"Authorization": "test_token"}

    setup_auto_tracing()
    client.get(reverse("active_tasks", args=("test",)), headers=headers)
    traces = get_trace()
    last_trace = traces[-1]
    # default django attributes
    assert last_trace.attributes["http.method"] == "GET"
    assert last_trace.attributes["http.url"].endswith("/test/tasks/")
    assert last_trace.attributes["http.status_code"] == 200
    # custom attributes
    assert last_trace.attributes["backend"] == "test"


def test_update_task_view_tracing(db, client, monkeypatch):
    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_TOKENS", {"test": "test_token"}
    )
    headers = {"Authorization": "test_token"}

    setup_auto_tracing()
    task = runjob_db_task_factory(backend="test")

    post_data = {
        "task_id": task.id,
        "stage": "prepared",
        "results": {},
        "complete": False,
        "timestamp_ns": "",
    }

    client.post(
        reverse("update_task", args=("test",)),
        data={"payload": json.dumps(post_data)},
        headers=headers,
    )
    traces = get_trace()
    last_trace = traces[-1]
    # default django attributes
    assert last_trace.attributes["http.method"] == "POST"
    assert last_trace.attributes["http.url"].endswith("/test/task/update/")
    assert last_trace.attributes["http.status_code"] == 200
    # custom attributes
    assert last_trace.attributes["backend"] == "test"
    assert last_trace.attributes["task_id"] == task.id
