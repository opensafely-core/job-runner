from django.http import JsonResponse
from django.urls import reverse

from jobrunner.lib import database
from jobrunner.models import Task
from tests.factories import (
    canceljob_db_task_factory,
    runjob_db_task_factory,
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


def test_update_task(db, client):
    make_task = runjob_db_task_factory(backend="test")

    assert make_task.agent_stage is None

    post_data = {
        "task_id": make_task.id,
        "stage": "prepared",
        "results": "",
        "complete": False,
    }

    response = client.post(reverse("update_task", args=("test",)), data=post_data)

    assert response.status_code == 200

    task = database.find_one(Task, id=make_task.id)

    assert task.agent_stage == "prepared"
