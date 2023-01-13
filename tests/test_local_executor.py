import time

import pytest

from jobrunner import config
from jobrunner.executors import local, volumes
from jobrunner.job_executor import ExecutorState, JobDefinition, Privacy, Study
from jobrunner.lib import datestr_to_ns_timestamp, docker
from tests.factories import ensure_docker_images_present


# this is parametized fixture, and test using it will run multiple times, once
# for each volume api implementation
@pytest.fixture(params=[volumes.BindMountVolumeAPI, volumes.DockerVolumeAPI])
def volume_api(request, monkeypatch):
    monkeypatch.setattr(local, "volume_api", request.param)
    return request.param


def populate_workspace(workspace, filename, content=None, privacy="high"):
    assert privacy in ("high", "medium")
    if privacy == "high":
        path = local.get_high_privacy_workspace(workspace) / filename
    else:
        path = local.get_medium_privacy_workspace(workspace) / filename

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content or filename)
    return path


# used for tests and debugging
def get_log(job):
    result = docker.docker(
        ["container", "logs", local.container_name(job)],
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout + result.stderr


def wait_for_state(api, job, state, limit=5, step=0.25):
    """Utility to wait on a state change in the api."""
    start = time.time()
    elapsed = 0

    while True:
        status = api.get_status(job)
        if status.state == state:
            return status

        elapsed = time.time() - start
        if elapsed > limit:
            raise Exception(f"Timed out waiting for state {state} for job {job}")

        time.sleep(step)


def list_repo_files(path):
    return list(str(f.relative_to(path)) for f in path.glob("**/*") if f.is_file())


@pytest.mark.needs_docker
def test_prepare_success(docker_cleanup, test_repo, tmp_work_dir, volume_api, freezer):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test-id",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={
            "*": "medium",
            "**/*": "medium",
        },
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "output/input.csv")

    expected_timestamp = time.time_ns()

    api = local.LocalDockerAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.PREPARED

    # we don't need to wait for this is currently synchronous
    next_status = api.get_status(job)

    assert next_status.state == ExecutorState.PREPARED
    assert next_status.timestamp_ns == expected_timestamp

    assert volume_api.volume_exists(job)

    # check files have been copied
    expected = set(list_repo_files(test_repo.source) + job.inputs)
    expected.add(local.TIMESTAMP_REFERENCE_FILE)

    # glob_volume_files uses find, and its '**/*' regex doesn't find files in
    # the root dir, which is arguably correct.
    files = volume_api.glob_volume_files(job)
    all_files = set(files["*"] + files["**/*"])
    assert all_files == expected


@pytest.mark.needs_docker
def test_prepare_already_prepared(docker_cleanup, test_repo, volume_api):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_prepare_already_prepared",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    # create the volume already
    volume_api.create_volume(job)
    volume_api.write_timestamp(job, local.TIMESTAMP_REFERENCE_FILE)

    api = local.LocalDockerAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.PREPARED


@pytest.mark.needs_docker
def test_prepare_no_image(docker_cleanup, test_repo, volume_api):
    job = JobDefinition(
        id="test_prepare_no_image",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="invalid-test-image",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    api = local.LocalDockerAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.ERROR
    assert job.image in status.message.lower()


@pytest.mark.parametrize("ext", config.ARCHIVE_FORMATS)
def test_prepare_archived(ext, test_repo):
    job = JobDefinition(
        id="test_prepare_archived",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    api = local.LocalDockerAPI()
    archive = (config.HIGH_PRIVACY_ARCHIVE_DIR / job.workspace).with_suffix(ext)
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.write_text("I exist")
    status = api.prepare(job)

    assert status.state == ExecutorState.ERROR
    assert "has been archived"


@pytest.mark.needs_docker
def test_prepare_job_bad_commit(docker_cleanup, test_repo):
    job = JobDefinition(
        id="test_prepare_job_bad_commit",
        job_request_id="test_request_id",
        study=Study(git_repo_url=str(test_repo.path), commit="bad-commit"),
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="invalid-test-image",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    with pytest.raises(local.LocalDockerError) as exc_info:
        local.prepare_job(job)

    assert job.study.commit in str(exc_info.value)


@pytest.mark.needs_docker
def test_prepare_job_no_input_file(docker_cleanup, test_repo, volume_api):
    job = JobDefinition(
        id="test_prepare_job_no_input_file",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    with pytest.raises(local.LocalDockerError) as exc_info:
        local.prepare_job(job)

    assert "output/input.csv" in str(exc_info.value)


@pytest.mark.needs_docker
def test_execute_success(docker_cleanup, test_repo, tmp_work_dir, volume_api):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_execute_success",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
        cpu_count=1.5,
        memory_limit="1G",
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerAPI()

    # use prepare step as test set up
    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARED

    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    # could be in either state
    assert api.get_status(job).state in (
        ExecutorState.EXECUTING,
        ExecutorState.EXECUTED,
    )

    container_data = docker.container_inspect(local.container_name(job), "HostConfig")
    assert container_data["NanoCpus"] == int(1.5 * 1e9)
    assert container_data["Memory"] == 2**30  # 1G


@pytest.mark.needs_docker
def test_execute_not_prepared(docker_cleanup, test_repo, tmp_work_dir, volume_api):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_execute_not_prepared",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    api = local.LocalDockerAPI()

    status = api.execute(job)
    # this will be turned into an error by the loop
    assert status.state == ExecutorState.UNKNOWN


@pytest.mark.needs_docker
def test_finalize_success(docker_cleanup, test_repo, tmp_work_dir, volume_api):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_finalize_success",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["touch", "/workspace/output/output.csv", "/workspace/output/summary.csv"],
        env={},
        inputs=["output/input.csv"],
        output_spec={
            "output/output.*": "high_privacy",
            "output/summary.*": "medium_privacy",
        },
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerAPI()

    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    status = wait_for_state(api, job, ExecutorState.EXECUTED)

    # check that timestamp is as expected
    container = docker.container_inspect(local.container_name(job))
    assert status.timestamp_ns == datestr_to_ns_timestamp(
        container["State"]["FinishedAt"]
    )

    status = api.finalize(job)
    assert status.state == ExecutorState.FINALIZED

    assert api.get_status(job).state == ExecutorState.FINALIZED
    assert job.id in local.RESULTS

    # for test debugging if any asserts fail
    print(get_log(job))
    results = api.get_results(job)
    assert results.exit_code == 0
    assert results.outputs == {
        "output/output.csv": "high_privacy",
        "output/summary.csv": "medium_privacy",
    }
    assert results.unmatched_patterns == []


@pytest.mark.needs_docker
def test_finalize_failed(docker_cleanup, test_repo, tmp_work_dir, volume_api):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_finalize_failed",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["false"],
        env={},
        inputs=["output/input.csv"],
        output_spec={
            "output/output.*": "high_privacy",
            "output/summary.*": "medium_privacy",
        },
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerAPI()

    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    wait_for_state(api, job, ExecutorState.EXECUTED)

    status = api.finalize(job)
    assert status.state == ExecutorState.FINALIZED

    # we don't need to wait
    assert api.get_status(job).state == ExecutorState.FINALIZED
    assert job.id in local.RESULTS

    # for test debugging if any asserts fail
    print(get_log(job))
    results = api.get_results(job)
    assert results.exit_code == 1
    assert results.outputs == {}
    assert results.unmatched_patterns == ["output/output.*", "output/summary.*"]


@pytest.mark.needs_docker
def test_finalize_unmatched(docker_cleanup, test_repo, tmp_work_dir, volume_api):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_finalize_unmatched",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        # the sleep is needed to make sure the unmatched file is *newer* enough
        args=["sh", "-c", "sleep 1; touch /workspace/unmatched"],
        env={},
        inputs=["output/input.csv"],
        output_spec={
            "output/output.*": "high_privacy",
            "output/summary.*": "medium_privacy",
        },
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerAPI()

    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    wait_for_state(api, job, ExecutorState.EXECUTED)

    status = api.finalize(job)
    assert status.state == ExecutorState.FINALIZED

    # we don't need to wait
    assert api.get_status(job).state == ExecutorState.FINALIZED
    assert job.id in local.RESULTS

    # for test debugging if any asserts fail
    print(get_log(job))
    results = api.get_results(job)
    assert results.exit_code == 0
    assert results.outputs == {}
    assert results.unmatched_patterns == ["output/output.*", "output/summary.*"]
    assert results.unmatched_outputs == ["unmatched"]


@pytest.mark.needs_docker
def test_finalize_failed_137(docker_cleanup, test_repo, tmp_work_dir, volume_api):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_finalize_failed",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["sleep", "101"],
        env={},
        inputs=["output/input.csv"],
        output_spec={
            "output/output.*": "high_privacy",
            "output/summary.*": "medium_privacy",
        },
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerAPI()

    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    # impersonate an admin
    docker.kill(local.container_name(job))

    wait_for_state(api, job, ExecutorState.EXECUTED)

    status = api.finalize(job)
    assert status.state == ExecutorState.FINALIZED

    # we don't need to wait
    assert api.get_status(job).state == ExecutorState.FINALIZED
    assert job.id in local.RESULTS
    assert local.RESULTS[job.id].exit_code == 137
    assert local.RESULTS[job.id].message == "Killed by an OpenSAFELY admin"


@pytest.mark.needs_docker
def test_finalize_failed_oomkilled(docker_cleanup, test_repo, tmp_work_dir):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_finalize_failed",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        # Consume memory by writing to the tmpfs at /dev/shm
        # We write a lot more that our limit, to ensure the OOM killer kicks in
        # regardless of our tests host's vm.overcommit_memory settings.
        args=["sh", "-c", "head -c 1000m /dev/urandom >/dev/shm/foo"],
        env={},
        inputs=["output/input.csv"],
        output_spec={
            "output/output.*": "high_privacy",
            "output/summary.*": "medium_privacy",
        },
        allow_database_access=False,
        memory_limit="6M",  # lowest allowable limit
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerAPI()

    status = api.prepare(job)
    status = api.execute(job)

    wait_for_state(api, job, ExecutorState.EXECUTED)

    status = api.finalize(job)
    assert status.state == ExecutorState.FINALIZED

    # we don't need to wait
    assert api.get_status(job).state == ExecutorState.FINALIZED
    assert job.id in local.RESULTS
    assert local.RESULTS[job.id].exit_code == 137
    # Note, 6MB is rounded to 0.01GBM by the formatter
    assert (
        local.RESULTS[job.id].message
        == "Ran out of memory (limit for this job was 0.01GB)"
    )


@pytest.mark.needs_docker
def test_cleanup_success(docker_cleanup, test_repo, tmp_work_dir, volume_api):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_cleanup_success",
        job_request_id="test_request_id",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerAPI()
    api.prepare(job)
    api.execute(job)

    container = local.container_name(job)
    assert volume_api.volume_exists(job)
    assert docker.container_exists(container)

    status = api.cleanup(job)
    assert status.state == ExecutorState.UNKNOWN

    status = api.get_status(job)
    assert status.state == ExecutorState.UNKNOWN

    assert not volume_api.volume_exists(job)
    assert not docker.container_exists(container)


def test_delete_files_success(tmp_work_dir):

    high = populate_workspace("test", "file.txt")
    medium = populate_workspace("test", "file.txt", privacy="medium")

    assert high.exists()
    assert medium.exists()

    api = local.LocalDockerAPI()
    errors = api.delete_files("test", Privacy.HIGH, ["file.txt"])

    # on windows, we cannot always delete, so check we tried to delete it
    if errors:
        assert errors == ["file.txt"]
    else:
        assert not high.exists()
    assert medium.exists()

    errors = api.delete_files("test", Privacy.MEDIUM, ["file.txt"])
    if errors:
        assert errors == ["file.txt"]
    else:
        assert not medium.exists()


def test_delete_files_error(tmp_work_dir):

    # use the fact that unlink() on a director raises an error
    populate_workspace("test", "bad/_")

    api = local.LocalDockerAPI()
    errors = api.delete_files("test", Privacy.HIGH, ["bad"])

    assert errors == ["bad"]


def test_delete_files_bad_privacy(tmp_work_dir):
    api = local.LocalDockerAPI()
    populate_workspace("test", "file.txt")
    with pytest.raises(Exception):
        api.delete_files("test", None, ["file.txt"])


def test_get_status_timeout(tmp_work_dir, monkeypatch):

    job = JobDefinition(
        id="test_get_status_timeout",
        job_request_id="test_request_id",
        study=None,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["sleep", "1"],
        env={},
        inputs=[],
        output_spec={},
        allow_database_access=False,
    )

    def inspect(*args, **kwargs):
        raise docker.DockerTimeoutError("timeout")

    monkeypatch.setattr(local.docker, "container_inspect", inspect)
    api = local.LocalDockerAPI()

    with pytest.raises(local.ExecutorRetry) as exc:
        api.get_status(job, timeout=11)

    assert (
        str(exc.value)
        == "docker timed out after 11s inspecting container os-job-test_get_status_timeout"
    )


@pytest.mark.needs_docker
def test_write_read_timestamps(tmp_work_dir, volume_api):

    job = JobDefinition(
        id="test_read_timestamp",
        job_request_id="test_request_id",
        study=None,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["sleep", "1"],
        env={},
        inputs=[],
        output_spec={},
        allow_database_access=False,
    )

    volume_api.create_volume(job)
    before = time.time_ns()
    volume_api.write_timestamp(job, "test")
    after = time.time_ns()
    ts = volume_api.read_timestamp(job, "test")

    assert before <= ts <= after


@pytest.mark.needs_docker
def test_read_timestamp_stat_fallback(tmp_work_dir):

    job = JobDefinition(
        id="test_read_timestamp_stat",
        job_request_id="test_request_id",
        study=None,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["sleep", "1"],
        env={},
        inputs=[],
        output_spec={},
        allow_database_access=False,
    )

    volumes.DockerVolumeAPI.create_volume(job)

    volume_name = volumes.DockerVolumeAPI.volume_name(job)
    before = time.time_ns()

    path = "test"
    # just touch the file, no contents
    docker.docker(
        [
            "container",
            "exec",
            docker.manager_name(volume_name),
            "touch",
            f"{docker.VOLUME_MOUNT_POINT}/{path}",
        ],
        check=True,
    )

    after = time.time_ns()
    ts = volumes.DockerVolumeAPI.read_timestamp(job, path)

    # check its ns value in the right range
    assert before <= ts <= after
