import json

from django.http import JsonResponse
from django.test import Client


def test_controller_returns_get_request_method():
    client = Client()
    response = client.get("/")
    json_response = json.loads(response.content)  # converts json objects to python dict
    assert json_response["method"] == "GET"
    assert isinstance(response, JsonResponse)


def test_controller_returns_post_request_method():
    client = Client()
    response = client.post("/")
    json_response = json.loads(response.content)
    assert json_response["method"] == "POST"
    assert isinstance(response, JsonResponse)
