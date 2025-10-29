import subprocess

import pytest

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
