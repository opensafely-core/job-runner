import pytest
import requests

from controller.lib import docker


pytestmark = pytest.mark.needs_ghcr


def test_dockerhub_api_refresh_functional(responses, monkeypatch):
    responses.add_passthru("https://ghcr.io/")

    # ensure there is a valid token cached
    response = docker.dockerhub_api("/v2/opensafely-core/busybox/manifests/latest")
    assert response.status_code == 200
    previous_token = docker.token

    # Add a mock to simulate the token being expired. We want to just mock the
    # first call, and let the 2nd passthru, to verify that our token
    # regeneration actually works against ghcr.io.
    #
    # However, due to combination of passthru *and* mocks, the behaviour here
    # is that mock is *not* consumed. So we have to consume it our selves
    # manually, which is awkard
    mock_ref = None

    def callback(request):
        responses.remove(mock_ref)
        return (
            401,
            {
                "www-authenticate": 'Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:opensafely-core/busybox:pull"',
            },
            "",
        )

    mock_ref = responses.add_callback(
        method="GET",
        url="https://ghcr.io/v2/opensafely-core/busybox/manifests/latest",
        callback=callback,
    )

    # make the call again, should now exercise the refresh logic
    response = docker.dockerhub_api("/v2/opensafely-core/busybox/manifests/latest")
    assert response.status_code == 200
    assert docker.token != previous_token


def test_get_current_image_sha_functional(responses):
    responses.add_passthru("https://ghcr.io/")
    sha = docker.get_current_image_sha("busybox:latest")
    assert sha.startswith("sha256:")
    assert len(sha) == 7 + 64


def test_get_current_image_sha_fallback_on_error(responses, monkeypatch):
    responses.add(
        method="GET",
        url="https://ghcr.io/v2/opensafely-core/busybox/manifests/latest",
        status=500,
    )
    # value doesn't matter, as we will error
    monkeypatch.setattr(docker, "token", "somevalue")
    monkeypatch.setitem(docker.docker_sha_cache, "busybox:latest", "oldsha")
    sha = docker.get_current_image_sha("busybox:latest")
    assert sha == "oldsha"


def test_get_current_image_sha_no_fallback_on_error(responses, monkeypatch):
    responses.add(
        method="GET",
        url="https://ghcr.io/v2/opensafely-core/busybox/manifests/latest",
        status=500,
    )
    # value doesn't matter, as we will error
    monkeypatch.setattr(docker, "token", "somevalue")
    monkeypatch.setattr(docker, "docker_sha_cache", {})
    with pytest.raises(requests.exceptions.RequestException):
        docker.get_current_image_sha("busybox:latest")
