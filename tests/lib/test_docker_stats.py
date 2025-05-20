import pytest
from opentelemetry import trace

from jobrunner.lib import docker, docker_stats


@pytest.fixture(autouse=True)
def clear_cache():
    docker_stats.CONTAINER_METADATA.clear()


@pytest.mark.needs_docker
def test_get_job_stats(docker_cleanup):
    docker.run("os-job-test", [docker.MANAGEMENT_CONTAINER_IMAGE, "sleep", "1000"])
    docker.run("not-job", [docker.MANAGEMENT_CONTAINER_IMAGE, "sleep", "1000"])
    containers = docker_stats.get_job_stats()
    assert isinstance(containers["test"]["cpu_percentage"], float)
    assert isinstance(containers["test"]["memory_used"], int)
    assert isinstance(containers["test"]["container_id"], str)
    assert isinstance(containers["test"]["started_at"], int)


@pytest.mark.needs_docker
def test_get_job_stats_cache(docker_cleanup):
    assert len(docker_stats.CONTAINER_METADATA) == 0
    docker_stats.CONTAINER_METADATA["stale_container"] = 0

    docker.run("os-job-test", [docker.MANAGEMENT_CONTAINER_IMAGE, "sleep", "1000"])
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("s1") as s1:
        docker_stats.get_job_stats()

    assert s1.attributes["container_cache_size"] == 1

    assert len(docker_stats.CONTAINER_METADATA) == 1
    assert "os-job-test" in docker_stats.CONTAINER_METADATA
    assert "state_container" not in docker_stats.CONTAINER_METADATA

    # for coverage of already cached branch
    with tracer.start_as_current_span("s2") as s2:
        docker_stats.get_job_stats()

    assert s2.attributes["container_cache_size"] == 1


def test_parse_job_id():
    assert docker_stats._parse_job_id("os-job-jobid") == "jobid"
    # we had a bug where we use str.lstrip("os-job-") instead of removeprefix :facepalm:
    assert docker_stats._parse_job_id("os-job-ojobid") == "ojobid"


def test_docker_datestr_to_int_timestamp():
    assert (
        docker_stats._docker_datestr_to_int_timestamp("2025-05-07T12:00:00.000000000Z")
        == 1746619200
    )
