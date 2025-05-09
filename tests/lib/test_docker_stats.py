import pytest

from jobrunner.lib import docker, docker_stats


@pytest.mark.needs_docker
def test_get_job_stats(docker_cleanup):
    docker.run("os-job-test", [docker.MANAGEMENT_CONTAINER_IMAGE, "sleep", "10"])
    containers = docker_stats.get_job_stats()
    assert isinstance(containers["test"]["cpu_percentage"], float)
    assert isinstance(containers["test"]["memory_used"], int)
    assert isinstance(containers["test"]["container_id"], str)
    assert isinstance(containers["test"]["started_at"], int)


def test_parse_job_id():
    assert docker_stats._parse_job_id("os-job-jobid") == "jobid"
    # we had a bug where we use str.lstrip("os-job-") instead of removeprefix :facepalm:
    assert docker_stats._parse_job_id("os-job-ojobid") == "ojobid"
