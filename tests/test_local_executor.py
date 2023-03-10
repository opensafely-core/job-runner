import logging
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
    monkeypatch.setattr(volumes, "DEFAULT_VOLUME_API", request.param)
    return request.param


@pytest.fixture
def job(request, test_repo):
    # TODO: this is a "JobDefinition", not a "Job"
    """Basic simple action with no inputs as base for testing."""
    if "needs_docker" in list(m.name for m in request.node.iter_markers()):
        ensure_docker_images_present("busybox")

    # replace parameterized tests [/] chars
    clean_name = request.node.name.replace("[", "_").replace("]", "_")
    return JobDefinition(
        id=clean_name,
        job_request_id=f"job-request-{clean_name}",
        study=test_repo.study,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        inputs=[],
        env={},
        # all files are outputs by default, for simplicity in tests
        output_spec={
            "*": "medium",
            "**/*": "medium",
        },
        allow_database_access=False,
    )


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


def workspace_log_file_exists(job):
    log_dir = local.get_log_dir(job)
    if not log_dir.exists():
        return False
    log_file = log_dir / "logs.txt"
    if not log_file.exists():
        return False

    workspace_log_file = (
        local.get_high_privacy_workspace(job.workspace)
        / local.METADATA_DIR
        / f"{job.action}.log"
    )
    return workspace_log_file.exists()


@pytest.mark.needs_docker
def test_prepare_success(
    docker_cleanup, job, test_repo, tmp_work_dir, volume_api, freezer
):

    job.inputs = ["output/input.csv"]
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
def test_prepare_already_prepared(docker_cleanup, job, volume_api):

    # create the volume already
    volume_api.create_volume(job)
    volume_api.write_timestamp(job, local.TIMESTAMP_REFERENCE_FILE)

    api = local.LocalDockerAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.PREPARED


@pytest.mark.needs_docker
def test_prepare_volume_exists_unprepared(docker_cleanup, job, volume_api):
    # create the volume already
    volume_api.create_volume(job)

    # do not write the timestamp, so prepare will rerun

    api = local.LocalDockerAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.PREPARED


@pytest.mark.needs_docker
def test_prepare_no_image(docker_cleanup, job, volume_api):
    job.image = "invalid-test-image"
    api = local.LocalDockerAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.ERROR
    assert job.image in status.message.lower()


@pytest.mark.needs_docker
@pytest.mark.parametrize("ext", config.ARCHIVE_FORMATS)
def test_prepare_archived(ext, job):
    api = local.LocalDockerAPI()
    archive = (config.HIGH_PRIVACY_ARCHIVE_DIR / job.workspace).with_suffix(ext)
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.write_text("I exist")
    status = api.prepare(job)

    assert status.state == ExecutorState.ERROR
    assert "has been archived"


@pytest.mark.needs_docker
def test_prepare_job_bad_commit(docker_cleanup, job, test_repo):
    job.study = Study(git_repo_url=str(test_repo.path), commit="bad-commit")

    with pytest.raises(local.LocalDockerError) as exc_info:
        local.prepare_job(job)

    assert job.study.commit in str(exc_info.value)


@pytest.mark.needs_docker
def test_prepare_job_no_input_file(docker_cleanup, job, volume_api):

    job.inputs = ["output/input.csv"]

    with pytest.raises(local.LocalDockerError) as exc_info:
        local.prepare_job(job)

    assert "output/input.csv" in str(exc_info.value)


@pytest.mark.needs_docker
def test_execute_success(docker_cleanup, job, tmp_work_dir, volume_api):

    # check limits are applied
    job.cpu_count = 1.5
    job.memory_limit = "1G"

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
def test_execute_not_prepared(docker_cleanup, job, tmp_work_dir, volume_api):
    api = local.LocalDockerAPI()

    status = api.execute(job)
    # this will be turned into an error by the loop
    assert status.state == ExecutorState.UNKNOWN


@pytest.mark.needs_docker
def test_finalize_success(docker_cleanup, job, tmp_work_dir, volume_api):

    job.args = [
        "touch",
        "/workspace/output/output.csv",
        "/workspace/output/summary.csv",
    ]
    job.inputs = ["output/input.csv"]
    job.output_spec = {
        "output/output.*": "high_privacy",
        "output/summary.*": "medium_privacy",
    }
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

    log_dir = local.get_log_dir(job)
    log_file = log_dir / "logs.txt"
    assert log_dir.exists()
    assert log_file.exists()


@pytest.mark.needs_docker
def test_finalize_failed(docker_cleanup, job, tmp_work_dir, volume_api):

    job.args = ["false"]
    job.output_spec = {
        "output/output.*": "high_privacy",
        "output/summary.*": "medium_privacy",
    }

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
def test_finalize_unmatched(docker_cleanup, job, tmp_work_dir, volume_api):

    # the sleep is needed to make sure the unmatched file is *newer* enough
    job.args = ["sh", "-c", "sleep 1; touch /workspace/unmatched"]
    job.output_spec = {
        "output/output.*": "high_privacy",
        "output/summary.*": "medium_privacy",
    }

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
def test_finalize_failed_137(docker_cleanup, job, tmp_work_dir, volume_api):

    job.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    # impersonate an admin
    docker.kill(local.container_name(job))

    # slightly strange that this is EXECUTED and not ERROR
    wait_for_state(api, job, ExecutorState.EXECUTED)

    # This does run finalize, because the cancelled bit is not set
    status = api.finalize(job)
    assert status.state == ExecutorState.FINALIZED

    # we don't need to wait
    assert api.get_status(job).state == ExecutorState.FINALIZED
    assert job.id in local.RESULTS
    assert local.RESULTS[job.id].exit_code == 137
    assert local.RESULTS[job.id].message == "Killed by an OpenSAFELY admin"

    assert workspace_log_file_exists(job)


@pytest.mark.needs_docker
def test_finalize_failed_oomkilled(docker_cleanup, job, tmp_work_dir):

    # Consume memory by writing to the tmpfs at /dev/shm
    # We write a lot more that our limit, to ensure the OOM killer kicks in
    # regardless of our tests host's vm.overcommit_memory settings.
    job.args = ["sh", "-c", "head -c 1000m /dev/urandom >/dev/shm/foo"]
    job.memory_limit = "6M"  # lowest allowable limit

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

    assert workspace_log_file_exists(job)


@pytest.mark.needs_docker
def test_pending_job_cancelled_not_finalized(docker_cleanup, job, tmp_work_dir):
    job.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    # user cancels the job before it's started
    status = api.terminate(job)
    assert status.state == ExecutorState.ERROR
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    # run.py does not run finalize for cancelled jobs
    # status = api.finalize(job)
    # assert status.state == ExecutorState.UNKNOWN
    # assert api.get_status(job).state == ExecutorState.UNKNOWN

    status = api.cleanup(job)
    assert status.state == ExecutorState.UNKNOWN
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    assert job.id not in local.RESULTS

    assert not workspace_log_file_exists(job)


@pytest.mark.needs_docker
def test_running_job_cancelled_finalized(docker_cleanup, job, tmp_work_dir):
    job.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARED
    assert api.get_status(job).state == ExecutorState.PREPARED

    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING
    assert api.get_status(job).state == ExecutorState.EXECUTING

    status = api.terminate(job)
    assert status.state == ExecutorState.ERROR
    assert api.get_status(job).state == ExecutorState.EXECUTED

    # run.py does not run finalize for cancelled jobs
    # status = api.finalize(job)
    # assert status.state == ExecutorState.FINALIZED
    # assert api.get_status(job).state == ExecutorState.FINALIZED

    status = api.cleanup(job)
    assert status.state == ExecutorState.UNKNOWN
    assert api.get_status(job).state == ExecutorState.UNKNOWN

    assert job.id not in local.RESULTS
    # TODO: should we write these values when finalizing a cancelled job?
    # assert local.RESULTS[job.id].exit_code == 137
    # assert local.RESULTS[job.id].message == "Killed by an OpenSAFELY admin"

    assert not workspace_log_file_exists(job)


@pytest.mark.needs_docker
def test_cleanup_success(docker_cleanup, job, tmp_work_dir, volume_api):

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


@pytest.mark.needs_docker
def test_get_status_timeout(tmp_work_dir, job, monkeypatch):
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
def test_write_read_timestamps(docker_cleanup, job, tmp_work_dir, volume_api):

    assert volume_api.read_timestamp(job, "test") is None

    volume_api.create_volume(job)
    before = time.time_ns()
    volume_api.write_timestamp(job, "test")
    after = time.time_ns()
    ts = volume_api.read_timestamp(job, "test")

    assert before <= ts <= after


@pytest.mark.needs_docker
def test_read_timestamp_stat_fallback(docker_cleanup, job, tmp_work_dir):

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


@pytest.mark.needs_docker
def test_get_volume_api(volume_api, job, tmp_work_dir):
    volume_api.create_volume(job)
    assert volumes.get_volume_api(job) == volume_api


@pytest.mark.needs_docker
def test_delete_volume(docker_cleanup, job, tmp_work_dir, volume_api):
    # check it doesn't error
    volume_api.delete_volume(job)

    # check it does remove volume
    volume_api.create_volume(job)
    volume_api.write_timestamp(job, local.TIMESTAMP_REFERENCE_FILE)

    volume_api.delete_volume(job)

    assert not volume_api.volume_exists(job)


def test_delete_volume_error_bindmount(
    docker_cleanup, job, tmp_work_dir, monkeypatch, caplog
):
    def error(*args, **kwargs):
        raise Exception("some error")

    caplog.set_level(logging.ERROR)
    monkeypatch.setattr(volumes.shutil, "rmtree", error)

    volumes.BindMountVolumeAPI.delete_volume(job)

    assert str(volumes.host_volume_path(job)) in caplog.records[-1].msg
    assert "some error" in caplog.records[-1].exc_text


def test_delete_volume_error_file_bindmount_skips_and_logs(job, caplog):
    caplog.set_level(logging.ERROR)

    # we can't easily manufacture a file permissions error, so we use
    # a different error to test our onerror handling code: directory does not
    # exist
    volumes.BindMountVolumeAPI.delete_volume(job)

    # check the error is logged
    path = str(volumes.host_volume_path(job))
    assert path in caplog.records[-1].msg
    # *not* an exception log, just an error one
    assert caplog.records[-1].exc_text is None
