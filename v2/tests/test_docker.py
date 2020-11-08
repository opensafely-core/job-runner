import subprocess

import pytest

from jobrunner import docker


@pytest.fixture(autouse=True, scope="module")
def cleanup():
    # Workaround for the fact that `monkeypatch` is only function-scoped.
    # Hopefully will be unnecessary soon. See:
    # https://github.com/pytest-dev/pytest/issues/363
    from _pytest.monkeypatch import MonkeyPatch

    label_for_tests = "jobrunner-test-R5o1iLu"
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr("jobrunner.docker.LABEL", label_for_tests)
    yield
    delete_docker_entities("container", label_for_tests)
    delete_docker_entities("volume", label_for_tests)
    monkeypatch.undo()


def delete_docker_entities(entity, label):
    extra_arg = "--all" if entity == "container" else ""
    subprocess.run(
        f"docker {entity} ls {extra_arg} --filter label={label} --quiet "
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
    volume = "jobrunner-volume-test"
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
