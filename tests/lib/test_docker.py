import subprocess

import pytest

from jobrunner.lib import docker


@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_basic_volume_interaction(tmp_path, docker_cleanup):
    files = ["test1.txt", "test2.json", "subdir/test3.txt", "subdir/test4.json"]
    for name in files:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    volume = "jobrunner-volume-test"
    docker.create_volume(volume)
    # Test no error is thrown if volume already exists
    docker.create_volume(volume)
    docker.copy_to_volume(volume, tmp_path, ".")
    matches = docker.glob_volume_files(volume, ["*.txt", "*.json"])
    assert matches == {"*.txt": ["test1.txt"], "*.json": ["test2.json"]}
    matches = docker.glob_volume_files(volume, ["subdir/*"])
    assert matches == {"subdir/*": ["subdir/test3.txt", "subdir/test4.json"]}
    docker.delete_volume(volume)
    # Test no error is thrown if volume is already deleted
    docker.delete_volume(volume)


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

    monkeypatch.setattr(docker, "subprocess_run", error(msg, None))
    with pytest.raises(docker.DockerDiskSpaceError):
        docker.docker([])

    monkeypatch.setattr(docker, "subprocess_run", error(None, msg))
    with pytest.raises(docker.DockerDiskSpaceError):
        docker.docker([])

    msg = msg.encode("utf8")

    monkeypatch.setattr(docker, "subprocess_run", error(msg, None))
    with pytest.raises(docker.DockerDiskSpaceError):
        docker.docker([])

    monkeypatch.setattr(docker, "subprocess_run", error(None, msg))
    with pytest.raises(docker.DockerDiskSpaceError):
        docker.docker([])
