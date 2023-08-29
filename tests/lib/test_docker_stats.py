import pytest

from jobrunner.lib import docker, docker_stats


@pytest.mark.needs_docker
def test_get_container_stats(docker_cleanup):
    docker.run("os-job-test", [docker.MANAGEMENT_CONTAINER_IMAGE, "sleep", "10"])
    containers = docker_stats.get_container_stats()
    assert isinstance(containers["test"]["cpu_percentage"], float)
    assert isinstance(containers["test"]["memory_used"], int)


@pytest.mark.needs_docker
def test_get_container_stats_regression(docker_cleanup):
    # we had a bug where we use str.lstrip("os-job-") :facepalm"
    # id starts with o
    docker.run("os-job-otest", [docker.MANAGEMENT_CONTAINER_IMAGE, "sleep", "10"])
    containers = docker_stats.get_container_stats()
    assert isinstance(containers["otest"]["cpu_percentage"], float)
    assert isinstance(containers["otest"]["memory_used"], int)
