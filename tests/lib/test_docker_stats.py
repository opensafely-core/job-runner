import pytest

from jobrunner.lib import docker, docker_stats


@pytest.mark.needs_docker
def test_get_container_stats(docker_cleanup):
    docker.run("os-job-test", [docker.MANAGEMENT_CONTAINER_IMAGE, "sleep", "10"])
    containers = docker_stats.get_container_stats()
    assert isinstance(containers["test"]["cpu_percentage"], float)
    assert isinstance(containers["test"]["memory_used"], int)
