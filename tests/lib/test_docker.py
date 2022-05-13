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


@pytest.mark.needs_docker
def test_copy_to_and_from_volume(tmp_path, docker_cleanup):
    volume = __name__
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    src.write_text("I exist")
    docker.create_volume(volume)
    docker.copy_to_volume(volume, src, "src.txt")
    docker.copy_from_volume(volume, "src.txt", dst)
    assert dst.read_text() == "I exist"
    assert len(list(tmp_path.glob("dst.txt*.tmp"))) == 0


@pytest.mark.needs_docker
def test_copy_to_volume_dereference_symlinks(tmp_path, docker_cleanup):
    volume = __name__
    target = tmp_path / "target.txt"
    target.write_text("target")
    link = tmp_path / "link.txt"
    link.symlink_to(target)
    dst = tmp_path / "dst.txt"

    docker.create_volume(volume)
    docker.copy_to_volume(volume, link, "src.txt")
    docker.copy_from_volume(volume, "src.txt", dst)
    assert not dst.is_symlink()
    assert dst.read_text() == "target"


@pytest.mark.needs_docker
def test_copy_from_volume_error(tmp_path, docker_cleanup, monkeypatch):
    volume = __name__
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"

    src.write_text("I exist")
    docker.create_volume(volume)
    docker.copy_to_volume(volume, src, "src.txt")

    def docker_error(*args, **kwards):
        raise Exception("oh noes")

    monkeypatch.setattr(docker, "docker", docker_error)

    with pytest.raises(Exception):
        docker.copy_from_volume(volume, "src.txt", dst)

    assert not dst.exists()
    assert len(list(tmp_path.glob("dst.txt*.tmp"))) == 0
