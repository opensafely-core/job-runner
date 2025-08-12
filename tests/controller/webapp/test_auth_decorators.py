import json

import pytest
from django.http import JsonResponse

from controller.webapp.views.auth.rap import get_backends_for_client_token


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setattr("common.config.BACKENDS", ["test", "foo"])
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})


@get_backends_for_client_token
def view(request, *, token_backends):
    return JsonResponse({"result": token_backends})


def test_get_backends_for_client_token(rf):
    request = rf.get("/", headers={"Authorization": "test_token"})
    response = view(request)
    assert response.status_code == 200
    assert json.loads(response.content.decode()) == {"result": ["test"]}


def test_get_backends_for_client_token_no_token(rf, monkeypatch):
    request = rf.get("/")
    response = view(request)
    assert response.status_code == 401
    assert json.loads(response.content.decode()) == {
        "error": "Unauthorized",
        "details": "No token provided",
    }


def test_get_backends_for_client_token_bad_token(rf, monkeypatch):
    request = rf.get("/", headers={"Authorization": "bad_token"})
    response = view(request)
    assert response.status_code == 401
    assert json.loads(response.content.decode()) == {
        "error": "Unauthorized",
        "details": "Invalid token",
    }
