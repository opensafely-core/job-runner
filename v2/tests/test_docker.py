import subprocess

import pytest

from jobrunner import docker


TEST_PREFIX = "jobrunner-test-R5o1iLu-"


@pytest.fixture(autouse=True, scope="module")
def cleanup():
    delete_test_containers_and_volumes()
    yield
    delete_test_containers_and_volumes()


def delete_test_containers_and_volumes():
    for entity in ("container", "volume"):
        extra_arg = "--all" if entity == "container" else ""
        subprocess.run(
            f"docker {entity} ls {extra_arg} --filter name={TEST_PREFIX} --quiet "
            f"| xargs --no-run-if-empty docker {entity} rm --force",
            check=True,
            shell=True,
        )


def test_basic_volume_interaction(tmp_path):
    files = ["test1.txt", "test2.json", "subdir/test3.txt", "subdir/test4.json"]
    for name in files:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    volume = TEST_PREFIX + "1"
    docker.create_volume(volume)
    # Test no error is thrown if volume already exists
    docker.create_volume(volume)
    docker.copy_to_volume(volume, tmp_path)
    matches = docker.glob_volume_files(volume, ["*.txt", "*.json"])
    assert matches == {"*.txt": ["test1.txt"], "*.json": ["test2.json"]}
    matches = docker.glob_volume_files(volume, ["subdir/*"])
    assert matches == {"subdir/*": ["subdir/test3.txt", "subdir/test4.json"]}
    docker.delete_volume(volume)
    # Test no error is thrown if volume is already deleted
    docker.delete_volume(volume)
