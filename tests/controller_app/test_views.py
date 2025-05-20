from django.http import JsonResponse
from django.urls import reverse

from jobrunner.lib import database
from jobrunner.models import Task
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


def test_active_tasks_view(db, client, monkeypatch):
    runtask = runjob_db_task_factory(backend="test")
    response = client.get(reverse("active_tasks", args=("test",)))
    response = response.json()
    assert response["tasks"] == [
        {
            "id": runtask.id,
            "backend": "test",
            "type": "runjob",
            "definition": runtask.definition,
            "created_at": runtask.created_at,
        }
    ]


def test_active_tasks_view_multiple_backends(db, client, monkeypatch):
    monkeypatch.setattr("jobrunner.config.common.BACKENDS", ["test", "foo"])
    monkeypatch.setattr(
        "jobrunner.config.controller.DEFAULT_JOB_CPU_COUNT", {"test": 1.0, "foo": 1.0}
    )
    monkeypatch.setattr(
        "jobrunner.config.controller.DEFAULT_JOB_MEMORY_LIMIT",
        {"test": "1G", "foo": "1G"},
    )

    # active tasks on test backend
    runtask1 = runjob_db_task_factory(backend="test")
    canceltask1 = canceljob_db_task_factory(backend="test")
    # inactive tasks on test backend
    runjob_db_task_factory(backend="test", active=False)
    canceltask2 = canceljob_db_task_factory(backend="test")
    canceltask2.active = False
    database.update(canceltask2)
    # active tasks on other backend
    runjob_db_task_factory(backend="foo")
    canceljob_db_task_factory(backend="foo")

    response = client.get(reverse("active_tasks", args=("test",)))
    response = response.json()

    assert {task["id"] for task in response["tasks"]} == {runtask1.id, canceltask1.id}


def test_active_tasks_unknown_backend(db, client):
    response = client.get(reverse("active_tasks", args=("foo",)))
    response = response.json()
    assert response["tasks"] == []


def test_update_task(db, client):
    make_task = runjob_db_task_factory(backend="test")

    assert make_task.agent_stage is None

    post_data = {
        "task_id": make_task.id,
        "stage": "prepared",
        "results": {},
        "complete": False,
    }

    response = client.post(reverse("update_task", args=("test",)), data=post_data)

    assert response.status_code == 204

    task = database.find_one(Task, id=make_task.id)

    assert task.agent_stage == "prepared"


def test_update_task_no_matching_task(db, client):
    post_data = {
        "task_id": "unknown-task-id",
        "stage": "prepared",
        "results": {},
        "complete": False,
    }

    response = client.post(reverse("update_task", args=("test",)), data=post_data)

    assert response.status_code == 500
    assert response.json()["error"] == "Error updating task"


def test_active_tasks_view_tracing(db, client, monkeypatch):
    setup_auto_tracing()
    client.get(reverse("active_tasks", args=("test",)))
    traces = get_trace()
    last_trace = traces[-1]
    # default django attributes
    assert last_trace.attributes["http.method"] == "GET"
    assert last_trace.attributes["http.url"].endswith("/test/tasks/")
    assert last_trace.attributes["http.status_code"] == 200
    # custom attributes
    assert last_trace.attributes["backend"] == "test"


def test_update_task_view_tracing(db, client, monkeypatch):
    setup_auto_tracing()
    task = runjob_db_task_factory(backend="test")

    post_data = {
        "task_id": task.id,
        "stage": "prepared",
        "results": {},
        "complete": False,
    }

    client.post(reverse("update_task", args=("test",)), data=post_data)
    traces = get_trace()
    last_trace = traces[-1]
    # default django attributes
    assert last_trace.attributes["http.method"] == "POST"
    assert last_trace.attributes["http.url"].endswith("/test/task/update/")
    assert last_trace.attributes["http.status_code"] == 204
    # custom attributes
    assert last_trace.attributes["backend"] == "test"
    assert last_trace.attributes["task_id"] == task.id
