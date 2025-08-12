import json
from dataclasses import dataclass

import pytest
from django.http import JsonResponse

from controller.webapp.views.auth.rap import get_backends_for_client_token
from controller.webapp.views.validators.dataclasses import RequestBody
from controller.webapp.views.validators.decorators import validate_request_body
from controller.webapp.views.validators.exceptions import APIValidationError


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setattr("common.config.BACKENDS", ["test", "foo"])
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})


@dataclass
class DummyRequestBody(RequestBody):
    foo: str

    @classmethod
    def from_request(cls, body_data):
        if body_data.get("foo") != "bar":
            raise APIValidationError("bad foo")
        return cls(foo=body_data["foo"])


@get_backends_for_client_token
@validate_request_body(DummyRequestBody)
def view(request, *, token_backends, request_obj: DummyRequestBody):
    return JsonResponse({"result": request_obj.foo})


def test_validate_request_body(rf):
    request = rf.post(
        "/",
        {"foo": "bar", "backend": "test"},
        headers={"Authorization": "test_token"},
        content_type="application/json",
    )
    response = view(request)
    assert response.status_code == 200
    assert json.loads(response.content.decode()) == {"result": "bar"}


def test_validate_request_body_validation_error(rf):
    request = rf.post(
        "/",
        {"foo": "foo", "backend": "test"},
        headers={"Authorization": "test_token"},
        content_type="application/json",
    )
    response = view(request)
    response.status_code == 400
    assert json.loads(response.content.decode()) == {
        "error": "Validation error",
        "details": "bad foo",
    }


def test_validate_request_body_bad_json(rf):
    request = rf.post(
        "/",
        "foo",
        headers={"Authorization": "test_token"},
        content_type="application/json",
    )
    response = view(request)
    response.status_code == 400
    assert json.loads(response.content.decode()) == {
        "error": "Validation error",
        "details": "could not parse JSON from request body",
    }


@validate_request_body(DummyRequestBody)
@get_backends_for_client_token
def view_with_bad_decorator_order(
    request, *, token_backends, request_obj: DummyRequestBody
):
    return JsonResponse({"result": request_obj.foo})


def test_bad_decorator_order(rf):
    request = rf.post(
        "/",
        {"foo": "bar", "backend": "test"},
        headers={"Authorization": "test_token"},
        content_type="application/json",
    )
    with pytest.raises(
        AssertionError,
        match=(
            "`token_backends` keyword argument not found; ensure that the @get_backends_for_client_token "
            "decorator is before the @validate_request_body on this function"
        ),
    ):
        view_with_bad_decorator_order(request)
