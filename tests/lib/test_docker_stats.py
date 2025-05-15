import pytest

from jobrunner.lib import docker, docker_stats


@pytest.mark.needs_docker
def test_get_job_stats(docker_cleanup):
    docker.run("os-job-test", [docker.MANAGEMENT_CONTAINER_IMAGE, "sleep", "1000"])
    docker.run("not-job", [docker.MANAGEMENT_CONTAINER_IMAGE, "sleep", "1000"])
    containers = docker_stats.get_job_stats()
    assert isinstance(containers["test"]["cpu_percentage"], float)
    assert isinstance(containers["test"]["memory_used"], int)
    assert isinstance(containers["test"]["container_id"], str)
    assert isinstance(containers["test"]["started_at"], int)


def test_parse_job_id():
    assert docker_stats._parse_job_id("os-job-jobid") == "jobid"
    # we had a bug where we use str.lstrip("os-job-") instead of removeprefix :facepalm:
    assert docker_stats._parse_job_id("os-job-ojobid") == "ojobid"


@pytest.mark.parametrize(
    "datestr, expected",
    [
        # docker sometimes returns UTC sometimes BST, can't tell why.
        ("2025-05-07 12:00:00 +0000 UTC", 1746619200),
        ("2025-05-07 13:00:00 +0100 BST", 1746619200),
    ],
)
def test_docker_datestr_to_int_timestamp(datestr, expected):
    assert docker_stats._docker_datestr_to_int_timestamp(datestr) == expected
