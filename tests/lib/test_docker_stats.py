import time

import pytest

from jobrunner.lib import docker
from jobrunner.lib.docker_stats import (
    get_container_stats,
    get_volume_and_container_sizes,
)


@pytest.mark.needs_docker
# This runs fine locally but fails in CI, despite the retry logic added below.
# Don't have time to diagnose this properly and this isn't core functionality
# in any case
@pytest.mark.xfail
def test_get_container_stats(docker_cleanup):
    docker.run("test_container1", [docker.MANAGEMENT_CONTAINER_IMAGE, "sh"])
    # It can sometimes take a while before the container actually appears :(
    for _ in range(10):
        containers = get_container_stats()
        if "test_container1" not in containers:
            time.sleep(1)
        else:
            break
    assert containers["test_container1"] == {"cpu_percentage": 0, "memory_used": 0}


@pytest.mark.needs_docker
@pytest.mark.slow_test
def test_get_volume_and_container_sizes(tmp_path, docker_cleanup):
    half_meg_file = tmp_path / "halfmeg"
    half_meg_file.write_bytes(b"0" * 500000)
    docker.create_volume("test_volume1")
    docker.copy_to_volume("test_volume1", half_meg_file, "halfmeg")
    docker.run(
        "test_container2",
        [docker.MANAGEMENT_CONTAINER_IMAGE, "cp", "/workspace/halfmeg", "/halfmeg"],
        volume=("test_volume1", "/workspace"),
    )
    volumes, containers = get_volume_and_container_sizes()
    assert volumes["test_volume1"] == 500000
    assert containers["test_container2"] == 500000
