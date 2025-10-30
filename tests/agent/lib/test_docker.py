import subprocess

import pytest

from agent.executors.local import get_proxy_image_sha
from agent.lib import docker


def test_disk_space_detection(monkeypatch):
    def error(stdout, stderr):
        def run(args, timeout, **kwargs):
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=args,
                output=stdout,
                stderr=stderr,
            )

        return run

    msg = "Error response from daemon: no space left on device"

    monkeypatch.setattr(docker.subprocess, "run", error(msg, None))
    with pytest.raises(docker.DockerDiskSpaceError):
        docker.docker([])

    monkeypatch.setattr(docker.subprocess, "run", error(None, msg))
    with pytest.raises(docker.DockerDiskSpaceError):
        docker.docker([])

    msg = msg.encode("utf8")

    monkeypatch.setattr(docker.subprocess, "run", error(msg, None))
    with pytest.raises(docker.DockerDiskSpaceError):
        docker.docker([])

    monkeypatch.setattr(docker.subprocess, "run", error(None, msg))
    with pytest.raises(docker.DockerDiskSpaceError):
        docker.docker([])


def test_get_network_config_args():
    args = docker.get_network_config_args(
        "jobrunner-db", target_url="http://localhost/foo"
    )
    assert args == [
        "--network",
        "jobrunner-db",
        "--dns",
        "192.0.2.0",
        "--add-host",
        "localhost:127.0.0.1",
    ]


def test_get_network_config_args_no_target_url():
    args = docker.get_network_config_args("jobrunner-db")
    assert args == [
        "--network",
        "jobrunner-db",
        "--dns",
        "192.0.2.0",
    ]


def test_get_network_config_args_target_url_has_ip():
    args = docker.get_network_config_args(
        "jobrunner-db", target_url="http://127.0.0.1/foo"
    )
    assert args == [
        "--network",
        "jobrunner-db",
        "--dns",
        "192.0.2.0",
    ]


def list_images(pattern):
    ps = docker.docker(
        ["images", "--format", "{{.Repository}}:{{.Tag}}"],
        check=True,
        capture_output=True,
        text=True,
    )

    images = ps.stdout.strip().splitlines()
    return [img for img in images if img.startswith(pattern)]


# cached sha values to save using the api in these tests. If we do ever update
# busybox, we should probably update these, but they should work regardless
CURRENT_BUSYBOX_SHA = (
    "sha256:dacd1aa51e0b27c0e36c4981a7a8d9d8ec2c4a74bf125c0a44d0709497a522e9"
)
PREVIOUS_BUSYBOX_SHA = (
    "sha256:febcf61cd6e1ac9628f6ac14fa40836d16f3c6ddef3b303ff0321606e55ddd0b"
)


@pytest.fixture
def remove_current_labels():
    # to try isolate this, we remove the user's local busybox:latest tags. This
    # is not ideal, but it should be up to date at the tests completion
    docker.docker(
        ["rmi", "-f", "docker-proxy.opensafely.org/opensafely-core/busybox:latest"]
    )


@pytest.mark.needs_docker
@pytest.mark.needs_ghcr
def test_ensure_docker_sha_present_no_version_exists(remove_current_labels):
    registry_image_with_label = "ghcr.io/opensafely-core/busybox:latest"

    proxy_image_with_sha = get_proxy_image_sha(
        registry_image_with_label, CURRENT_BUSYBOX_SHA
    )
    # image with label is same for both
    proxy_image_with_label, _, _ = proxy_image_with_sha.partition("@")

    docker.ensure_docker_sha_present(proxy_image_with_sha, registry_image_with_label)

    image_metadata = docker.image_inspect(proxy_image_with_sha)
    assert docker.image_inspect(proxy_image_with_label) == image_metadata
    assert docker.image_inspect(registry_image_with_label) == image_metadata

    # check idempotance
    docker.ensure_docker_sha_present(proxy_image_with_sha, registry_image_with_label)
    assert docker.image_inspect(proxy_image_with_sha) == image_metadata
    assert docker.image_inspect(proxy_image_with_label) == image_metadata
    assert docker.image_inspect(registry_image_with_label) == image_metadata


@pytest.mark.needs_docker
@pytest.mark.needs_ghcr
def test_ensure_docker_sha_present_image_exists(remove_current_labels):
    registry_image_with_label = "ghcr.io/opensafely-core/busybox:latest"
    proxy_image_with_old_sha = get_proxy_image_sha(
        registry_image_with_label, PREVIOUS_BUSYBOX_SHA
    )
    proxy_image_with_new_sha = get_proxy_image_sha(
        registry_image_with_label, CURRENT_BUSYBOX_SHA
    )
    # image with label is same for both
    proxy_image_with_label, _, _ = proxy_image_with_old_sha.partition("@")

    docker.ensure_docker_sha_present(
        proxy_image_with_old_sha, registry_image_with_label
    )

    image_metadata = docker.image_inspect(proxy_image_with_old_sha)
    assert docker.image_inspect(proxy_image_with_label) == image_metadata
    assert docker.image_inspect(registry_image_with_label) == image_metadata

    docker.ensure_docker_sha_present(
        proxy_image_with_new_sha, registry_image_with_label
    )

    image_metadata = docker.image_inspect(proxy_image_with_new_sha)
    assert docker.image_inspect(proxy_image_with_label) == image_metadata
    assert docker.image_inspect(registry_image_with_label) == image_metadata


@pytest.mark.needs_docker
@pytest.mark.needs_ghcr
def test_ensure_docker_sha_present_old_image_doesnot_update_tag(remove_current_labels):
    registry_image_with_label = "ghcr.io/opensafely-core/busybox:latest"
    proxy_image_with_old_sha = get_proxy_image_sha(
        registry_image_with_label, PREVIOUS_BUSYBOX_SHA
    )
    proxy_image_with_new_sha = get_proxy_image_sha(
        registry_image_with_label, CURRENT_BUSYBOX_SHA
    )
    # image with label is same for both
    proxy_image_with_label, _, _ = proxy_image_with_old_sha.partition("@")

    docker.ensure_docker_sha_present(
        proxy_image_with_new_sha, registry_image_with_label
    )

    image_metadata = docker.image_inspect(proxy_image_with_new_sha)
    assert docker.image_inspect(proxy_image_with_label) == image_metadata
    assert docker.image_inspect(registry_image_with_label) == image_metadata

    docker.ensure_docker_sha_present(
        proxy_image_with_old_sha, registry_image_with_label
    )

    # assert label is still pointing to newer sha, has not been overwritten to
    # point to old sha
    image_metadata = docker.image_inspect(proxy_image_with_new_sha)
    assert docker.image_inspect(proxy_image_with_label) == image_metadata
    assert docker.image_inspect(registry_image_with_label) == image_metadata
