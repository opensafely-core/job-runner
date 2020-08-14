import subprocess

import pytest

from runner.utils import docker_container_exists, make_volume_name, safe_join


def test_safe_path():
    safe_join("/workdir", "file.txt") == "/workdir/file.txt"


def test_unsafe_paths_raise():
    with pytest.raises(AssertionError):
        safe_join("/workdir", "../file.txt")
    with pytest.raises(AssertionError):
        safe_join("/workdir", "/file.txt")


def test_make_volume_name():
    workspace = {
        "repo": "https://github.com/opensafely/hiv-research/",
        "name": "tofu",
        "branch": "feasibility-no",
        "owner": "me",
        "db": "full",
    }
    assert (
        make_volume_name(workspace)
        == "https-github-com-opensafely-hiv-research-feasibility-no-full-me-tofu"
    )


def xtest_job_runner_docker_container_exists(mock_env):
    """Tests the ability to see if a container is running or not.

    This test is slow: it depends on a docker install and network
    access, and the teardown in the last line blocks for a few seconds

    """
    assert not docker_container_exists("nonexistent_container_name")

    # Start a trivial docker container
    name = "existent_container_name"
    subprocess.check_call(
        ["docker", "run", "--detach", "--rm", "--name", name, "alpine", "sleep", "60"],
    )
    assert docker_container_exists(name)
    subprocess.check_call(["docker", "stop", name])
