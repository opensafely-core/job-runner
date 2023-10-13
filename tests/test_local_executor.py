import logging
import sys
import time

import pytest

from jobrunner import config
from jobrunner.executors import local, volumes
from jobrunner.job_executor import ExecutorState, JobDefinition, Privacy, Study
from jobrunner.lib import datestr_to_ns_timestamp, docker
from tests.conftest import SUPPORTED_VOLUME_APIS
from tests.factories import ensure_docker_images_present


# this is parametized fixture, and test using it will run multiple times, once
# for each volume api implementation
@pytest.fixture(params=SUPPORTED_VOLUME_APIS)
def volume_api(request, monkeypatch):
    monkeypatch.setattr(volumes, "DEFAULT_VOLUME_API", request.param)
    return request.param


@pytest.fixture
def job_definition(request, test_repo):
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
        args=["true"],
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
def get_log(job_definition):
    result = docker.docker(
        ["container", "logs", local.container_name(job_definition)],
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout + result.stderr


def wait_for_state(api, job_definition, state, limit=5, step=0.25):
    """Utility to wait on a state change in the api."""
    start = time.time()
    elapsed = 0

    while True:
        status = api.get_status(job_definition)
        if status.state == state:
            return status

        elapsed = time.time() - start
        if elapsed > limit:
            raise Exception(
                f"Timed out waiting for state {state} for job {job_definition}"
            )

        time.sleep(step)


def list_repo_files(path):
    return list(str(f.relative_to(path)) for f in path.glob("**/*") if f.is_file())


def log_dir_log_file_exists(job_definition):
    log_dir = local.get_log_dir(job_definition)
    if not log_dir.exists():
        return False
    log_file = log_dir / "logs.txt"
    return log_file.exists()


def workspace_log_file_exists(job_definition):
    workspace_log_file = (
        local.get_high_privacy_workspace(job_definition.workspace)
        / local.METADATA_DIR
        / f"{job_definition.action}.log"
    )
    return workspace_log_file.exists()


@pytest.mark.needs_docker
def test_prepare_success(
    docker_cleanup, job_definition, test_repo, tmp_work_dir, volume_api, freezer
):

    job_definition.inputs = ["output/input.csv"]
    populate_workspace(job_definition.workspace, "output/input.csv")

    expected_timestamp = time.time_ns()

    api = local.LocalDockerAPI()
    status = api.prepare(job_definition)

    assert status.state == ExecutorState.PREPARED

    # we don't need to wait for this is currently synchronous
    next_status = api.get_status(job_definition)

    assert next_status.state == ExecutorState.PREPARED
    assert next_status.timestamp_ns == expected_timestamp

    assert volume_api.volume_exists(job_definition)

    # check files have been copied
    expected = set(list_repo_files(test_repo.source) + job_definition.inputs)
    expected.add(local.TIMESTAMP_REFERENCE_FILE)

    # glob_volume_files uses find, and its '**/*' regex doesn't find files in
    # the root dir, which is arguably correct.
    files = volume_api.glob_volume_files(job_definition)
    all_files = set(files["*"] + files["**/*"])
    assert all_files == expected


@pytest.mark.needs_docker
def test_prepare_already_prepared(docker_cleanup, job_definition, volume_api):

    # create the volume already
    volume_api.create_volume(job_definition)
    volume_api.write_timestamp(job_definition, local.TIMESTAMP_REFERENCE_FILE)

    api = local.LocalDockerAPI()
    status = api.prepare(job_definition)

    assert status.state == ExecutorState.PREPARED


@pytest.mark.needs_docker
def test_prepare_volume_exists_unprepared(docker_cleanup, job_definition, volume_api):
    # create the volume already
    volume_api.create_volume(job_definition)

    # do not write the timestamp, so prepare will rerun

    api = local.LocalDockerAPI()
    status = api.prepare(job_definition)

    assert status.state == ExecutorState.PREPARED


@pytest.mark.needs_docker
def test_prepare_no_image(docker_cleanup, job_definition, volume_api):
    job_definition.image = "invalid-test-image"
    api = local.LocalDockerAPI()
    status = api.prepare(job_definition)

    assert status.state == ExecutorState.ERROR
    assert job_definition.image in status.message.lower()


@pytest.mark.needs_docker
@pytest.mark.parametrize("ext", config.ARCHIVE_FORMATS)
def test_prepare_archived(ext, job_definition):
    api = local.LocalDockerAPI()
    archive = (config.HIGH_PRIVACY_ARCHIVE_DIR / job_definition.workspace).with_suffix(
        ext
    )
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.write_text("I exist")
    status = api.prepare(job_definition)

    assert status.state == ExecutorState.ERROR
    assert "has been archived"


@pytest.mark.needs_docker
def test_prepare_job_bad_commit(docker_cleanup, job_definition, test_repo):
    job_definition.study = Study(git_repo_url=str(test_repo.path), commit="bad-commit")

    with pytest.raises(local.LocalDockerError) as exc_info:
        local.prepare_job(job_definition)

    assert job_definition.study.commit in str(exc_info.value)


@pytest.mark.needs_docker
def test_prepare_job_no_input_file(docker_cleanup, job_definition, volume_api):

    job_definition.inputs = ["output/input.csv"]

    with pytest.raises(local.LocalDockerError) as exc_info:
        local.prepare_job(job_definition)

    assert "output/input.csv" in str(exc_info.value)


@pytest.mark.needs_docker
def test_execute_success(docker_cleanup, job_definition, tmp_work_dir, volume_api):

    # check limits are applied
    job_definition.cpu_count = 1.5
    job_definition.memory_limit = "1G"

    api = local.LocalDockerAPI()

    # use prepare step as test set up
    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED

    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    # could be in either state
    assert api.get_status(job_definition).state in (
        ExecutorState.EXECUTING,
        ExecutorState.EXECUTED,
    )

    container_data = docker.container_inspect(local.container_name(job_definition))
    assert container_data["State"]["ExitCode"] == 0
    assert container_data["HostConfig"]["NanoCpus"] == int(1.5 * 1e9)
    assert container_data["HostConfig"]["Memory"] == 2**30  # 1G


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin", reason="linux/darwin only"
)
@pytest.mark.needs_docker
def test_execute_user_bindmount(
    docker_cleanup, job_definition, tmp_work_dir, monkeypatch
):
    monkeypatch.setattr(volumes, "DEFAULT_VOLUME_API", volumes.BindMountVolumeAPI)
    api = local.LocalDockerAPI()
    # use prepare step as test set up
    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED

    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    # could be in either state
    assert api.get_status(job_definition).state in (
        ExecutorState.EXECUTING,
        ExecutorState.EXECUTED,
    )

    container_config = docker.container_inspect(local.container_name(job_definition))

    # do not test that this config is set on platforms that do not require this config
    if config.DOCKER_USER_ID and config.DOCKER_GROUP_ID:
        assert (
            container_config["Config"]["User"]
            == f"{config.DOCKER_USER_ID}:{config.DOCKER_GROUP_ID}"
        )
    assert container_config["State"]["ExitCode"] == 0


@pytest.mark.needs_docker
def test_execute_not_prepared(docker_cleanup, job_definition, tmp_work_dir, volume_api):
    api = local.LocalDockerAPI()

    status = api.execute(job_definition)
    # this will be turned into an error by the loop
    assert status.state == ExecutorState.UNKNOWN


@pytest.mark.needs_docker
def test_finalize_success(docker_cleanup, job_definition, tmp_work_dir, volume_api):

    job_definition.args = [
        "touch",
        "/workspace/output/output.csv",
        "/workspace/output/summary.csv",
    ]
    job_definition.inputs = ["output/input.csv"]
    job_definition.output_spec = {
        "output/output.*": "highly_sensitive",
        "output/summary.*": "moderately_sensitive",
    }
    populate_workspace(job_definition.workspace, "output/input.csv")

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    status = wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    # check that timestamp is as expected
    container = docker.container_inspect(local.container_name(job_definition))
    assert status.timestamp_ns == datestr_to_ns_timestamp(
        container["State"]["FinishedAt"]
    )

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert api.get_status(job_definition).state == ExecutorState.FINALIZED
    assert job_definition.id in local.RESULTS

    # for test debugging if any asserts fail
    print(get_log(job_definition))
    results = api.get_results(job_definition)
    assert results.exit_code == 0
    assert results.outputs == {
        "output/output.csv": "highly_sensitive",
        "output/summary.csv": "moderately_sensitive",
    }
    assert results.unmatched_patterns == []

    log_dir = local.get_log_dir(job_definition)
    log_file = log_dir / "logs.txt"
    assert log_dir.exists()
    assert log_file.exists()


@pytest.mark.needs_docker
def test_finalize_failed(docker_cleanup, job_definition, tmp_work_dir, volume_api):

    job_definition.args = ["false"]
    job_definition.output_spec = {
        "output/output.*": "highly_sensitive",
        "output/summary.*": "moderately_sensitive",
    }

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED

    # we don't need to wait
    assert api.get_status(job_definition).state == ExecutorState.FINALIZED
    assert job_definition.id in local.RESULTS

    # for test debugging if any asserts fail
    print(get_log(job_definition))
    results = api.get_results(job_definition)
    assert results.exit_code == 1
    assert results.outputs == {}
    assert results.unmatched_patterns == ["output/output.*", "output/summary.*"]


@pytest.mark.needs_docker
def test_finalize_unmatched(docker_cleanup, job_definition, tmp_work_dir, volume_api):

    # the sleep is needed to make sure the unmatched file is *newer* enough
    job_definition.args = ["sh", "-c", "sleep 1; touch /workspace/unmatched"]
    job_definition.output_spec = {
        "output/output.*": "highly_sensitive",
        "output/summary.*": "moderately_sensitive",
    }

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED

    # we don't need to wait
    assert api.get_status(job_definition).state == ExecutorState.FINALIZED
    assert job_definition.id in local.RESULTS

    # for test debugging if any asserts fail
    print(get_log(job_definition))
    results = api.get_results(job_definition)
    assert results.exit_code == 0
    assert results.outputs == {}
    assert results.unmatched_patterns == ["output/output.*", "output/summary.*"]
    assert results.unmatched_outputs == ["unmatched"]


@pytest.mark.needs_docker
def test_finalize_failed_137(docker_cleanup, job_definition, tmp_work_dir, volume_api):

    job_definition.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    # impersonate an admin
    docker.kill(local.container_name(job_definition))

    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED
    # we don't need to wait
    assert api.get_status(job_definition).state == ExecutorState.FINALIZED

    assert job_definition.id in local.RESULTS
    assert local.RESULTS[job_definition.id].exit_code == 137
    assert (
        local.RESULTS[job_definition.id].message
        == "Job killed by OpenSAFELY admin or memory limits"
    )

    assert log_dir_log_file_exists(job_definition)
    assert workspace_log_file_exists(job_definition)


@pytest.mark.needs_docker
def test_finalize_failed_oomkilled(docker_cleanup, job_definition, tmp_work_dir):

    # Consume memory by writing to the tmpfs at /dev/shm
    # We write a lot more that our limit, to ensure the OOM killer kicks in
    # regardless of our tests host's vm.overcommit_memory settings.
    job_definition.args = ["sh", "-c", "head -c 1000m /dev/urandom >/dev/shm/foo"]
    job_definition.memory_limit = "6M"  # lowest allowable limit

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    status = api.execute(job_definition)

    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED

    # we don't need to wait
    assert api.get_status(job_definition).state == ExecutorState.FINALIZED
    assert job_definition.id in local.RESULTS
    assert local.RESULTS[job_definition.id].exit_code == 137
    # Note, 6MB is rounded to 0.01GBM by the formatter
    assert (
        local.RESULTS[job_definition.id].message
        == "Job ran out of memory (limit was 0.01GB)"
    )

    assert log_dir_log_file_exists(job_definition)
    assert workspace_log_file_exists(job_definition)


@pytest.mark.needs_docker
def test_finalize_large_level4_outputs(
    docker_cleanup, job_definition, tmp_work_dir, volume_api
):
    job_definition.args = [
        "truncate",
        "-s",
        str(1024 * 1024),
        "/workspace/output/output.txt",
    ]
    job_definition.output_spec = {
        "output/output.txt": "moderately_sensitive",
    }
    job_definition.level4_max_filesize = 512 * 1024

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    status = wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED

    result = api.get_results(job_definition)

    assert result.exit_code == 0
    assert result.level4_excluded_files == {
        "output/output.txt": "File size of 1.0Mb is larger that limit of 0.5Mb.",
    }

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)
    message_file = level4_dir / "output/output.txt.txt"
    txt = message_file.read_text()
    assert "output/output.txt" in txt
    assert "1.0Mb" in txt
    assert "0.5Mb" in txt
    log_file = level4_dir / "metadata/action.log"
    log = log_file.read_text()
    assert "excluded files:" in log
    assert "output/output.txt: File size of 1.0Mb is larger that limit of 0.5Mb." in log


@pytest.mark.needs_docker
def test_finalize_invalid_file_type(docker_cleanup, job_definition, tmp_work_dir):
    job_definition.args = ["touch", "/workspace/output/output.rds"]
    job_definition.output_spec = {
        "output/output.rds": "moderately_sensitive",
    }
    job_definition.level4_file_types = [".csv"]

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    status = wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED

    result = api.get_results(job_definition)

    assert result.exit_code == 0
    assert result.level4_excluded_files == {
        "output/output.rds": "File type of .rds is not valid level 4 file",
    }

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)
    message_file = level4_dir / "output/output.rds.txt"
    txt = message_file.read_text()
    assert "output/output.rds" in txt

    log_file = level4_dir / "metadata/action.log"
    log = log_file.read_text()
    assert "excluded files:" in log
    assert "output/output.rds: File type of .rds is not valid level 4 file" in log


@pytest.mark.needs_docker
def test_finalize_patient_id_header(docker_cleanup, job_definition, tmp_work_dir):
    job_definition.args = [
        "sh",
        "-c",
        "echo 'patient_id,foo,bar\n1,2,3' > /workspace/output/output.csv",
    ]
    job_definition.output_spec = {
        "output/output.csv": "moderately_sensitive",
    }
    job_definition.level4_file_types = [".csv"]

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    status = wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED

    result = api.get_results(job_definition)

    assert result.exit_code == 0
    assert result.level4_excluded_files == {
        "output/output.csv": "File has patient_id column",
    }

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)
    message_file = level4_dir / "output/output.csv.txt"
    txt = message_file.read_text()
    assert "output/output.csv" in txt
    assert "patient_id" in txt

    log_file = level4_dir / "metadata/action.log"
    log = log_file.read_text()
    assert "excluded files:" in log
    assert "output/output.csv: File has patient_id column" in log


@pytest.mark.needs_docker
def test_finalize_large_level4_outputs_cleanup(
    docker_cleanup, job_definition, tmp_work_dir, volume_api
):
    job_definition.args = [
        "truncate",
        "-s",
        str(256 * 1024),
        "/workspace/output/output.txt",
    ]
    job_definition.output_spec = {
        "output/output.txt": "moderately_sensitive",
    }
    job_definition.level4_max_filesize = 512 * 1024

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)
    message_file = level4_dir / "output/output.txt.txt"
    message_file.parent.mkdir(exist_ok=True, parents=True)
    message_file.write_text("message")

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING

    status = wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED

    result = api.get_results(job_definition)

    assert result.exit_code == 0
    assert result.level4_excluded_files == {}
    assert not message_file.exists()


@pytest.mark.needs_docker
def test_pending_job_terminated_not_finalized(
    docker_cleanup, job_definition, tmp_work_dir
):
    job_definition.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    # user cancels the job before it's started
    status = api.terminate(job_definition)
    job_definition.cancelled = "user"
    assert status.state == ExecutorState.UNKNOWN
    assert api.get_status(job_definition).state == ExecutorState.UNKNOWN

    # nb. no need to run terminate(), finalize() or cleanup()

    assert job_definition.id not in local.RESULTS
    assert not log_dir_log_file_exists(job_definition)
    assert not workspace_log_file_exists(job_definition)


@pytest.mark.needs_docker
def test_prepared_job_terminated_not_finalized(
    docker_cleanup, job_definition, tmp_work_dir
):
    job_definition.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    assert api.get_status(job_definition).state == ExecutorState.PREPARED

    job_definition.cancelled = "user"

    # nb. do not run terminate() or finalize() because we do not have a container

    assert api.get_status(job_definition).state == ExecutorState.FINALIZED

    assert job_definition.id not in local.RESULTS

    status = api.cleanup(job_definition)
    assert status.state == ExecutorState.UNKNOWN
    assert api.get_status(job_definition).state == ExecutorState.UNKNOWN

    assert not log_dir_log_file_exists(job_definition)
    assert not workspace_log_file_exists(job_definition)


@pytest.mark.needs_docker
def test_running_job_terminated_finalized(docker_cleanup, job_definition, tmp_work_dir):
    job_definition.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    status = api.prepare(job_definition)
    assert status.state == ExecutorState.PREPARED
    assert api.get_status(job_definition).state == ExecutorState.PREPARED

    status = api.execute(job_definition)
    assert status.state == ExecutorState.EXECUTING
    assert api.get_status(job_definition).state == ExecutorState.EXECUTING

    job_definition.cancelled = "user"
    status = api.terminate(job_definition)
    assert status.state == ExecutorState.EXECUTED
    assert api.get_status(job_definition).state == ExecutorState.EXECUTED

    status = api.finalize(job_definition)
    assert status.state == ExecutorState.FINALIZED
    assert api.get_status(job_definition).state == ExecutorState.FINALIZED

    assert job_definition.id in local.RESULTS
    assert local.RESULTS[job_definition.id].exit_code == 137
    assert local.RESULTS[job_definition.id].message == "Job cancelled by user"

    status = api.cleanup(job_definition)
    assert status.state == ExecutorState.UNKNOWN
    assert api.get_status(job_definition).state == ExecutorState.UNKNOWN

    assert job_definition.id not in local.RESULTS

    assert log_dir_log_file_exists(job_definition)
    assert not workspace_log_file_exists(job_definition)


@pytest.mark.needs_docker
def test_cleanup_success(docker_cleanup, job_definition, tmp_work_dir, volume_api):

    populate_workspace(job_definition.workspace, "output/input.csv")

    api = local.LocalDockerAPI()
    api.prepare(job_definition)
    api.execute(job_definition)

    container = local.container_name(job_definition)
    assert volume_api.volume_exists(job_definition)
    assert docker.container_exists(container)

    status = api.cleanup(job_definition)
    assert status.state == ExecutorState.UNKNOWN

    status = api.get_status(job_definition)
    assert status.state == ExecutorState.UNKNOWN

    assert not volume_api.volume_exists(job_definition)
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
def test_get_status_timeout(tmp_work_dir, job_definition, monkeypatch):
    def inspect(*args, **kwargs):
        raise docker.DockerTimeoutError("timeout")

    monkeypatch.setattr(local.docker, "container_inspect", inspect)
    api = local.LocalDockerAPI()

    with pytest.raises(local.ExecutorRetry) as exc:
        api.get_status(job_definition, timeout=11)

    assert (
        str(exc.value)
        == "docker timed out after 11s inspecting container os-job-test_get_status_timeout"
    )


@pytest.mark.needs_docker
def test_write_read_timestamps(
    docker_cleanup, job_definition, tmp_work_dir, volume_api
):

    assert volume_api.read_timestamp(job_definition, "test") is None

    volume_api.create_volume(job_definition)
    before = time.time_ns()
    volume_api.write_timestamp(job_definition, "test")
    after = time.time_ns()
    ts = volume_api.read_timestamp(job_definition, "test")

    assert before <= ts <= after


@pytest.mark.needs_docker
def test_read_timestamp_stat_fallback(docker_cleanup, job_definition, tmp_work_dir):

    volumes.DockerVolumeAPI.create_volume(job_definition)

    volume_name = volumes.DockerVolumeAPI.volume_name(job_definition)
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
    ts = volumes.DockerVolumeAPI.read_timestamp(job_definition, path)

    # check its ns value in the right range
    assert before <= ts <= after


@pytest.mark.needs_docker
def test_get_volume_api(docker_cleanup, volume_api, job_definition, tmp_work_dir):
    volume_api.create_volume(job_definition)
    assert volumes.get_volume_api(job_definition) == volume_api


@pytest.mark.needs_docker
def test_delete_volume(docker_cleanup, job_definition, tmp_work_dir, volume_api):
    # check it doesn't error
    volume_api.delete_volume(job_definition)

    # check it does remove volume
    volume_api.create_volume(job_definition)
    volume_api.write_timestamp(job_definition, local.TIMESTAMP_REFERENCE_FILE)

    volume_api.delete_volume(job_definition)

    assert not volume_api.volume_exists(job_definition)


@pytest.mark.skipif(
    sys.platform != "linux" and sys.platform != "darwin", reason="linux/darwin only"
)
def test_delete_volume_error_bindmount(
    docker_cleanup, job_definition, tmp_work_dir, monkeypatch, caplog
):
    def error(*args, **kwargs):
        raise Exception("some error")

    caplog.set_level(logging.ERROR)
    monkeypatch.setattr(volumes.shutil, "rmtree", error)

    volumes.BindMountVolumeAPI.delete_volume(job_definition)

    assert str(volumes.host_volume_path(job_definition)) in caplog.records[-1].msg
    assert "some error" in caplog.records[-1].exc_text


def test_delete_volume_error_file_bindmount_skips_and_logs(job_definition, caplog):
    caplog.set_level(logging.ERROR)

    # we can't easily manufacture a file permissions error, so we use
    # a different error to test our onerror handling code: directory does not
    # exist
    volumes.BindMountVolumeAPI.delete_volume(job_definition)

    # check the error is logged
    path = str(volumes.host_volume_path(job_definition))
    assert path in caplog.records[-1].msg
    # *not* an exception log, just an error one
    assert caplog.records[-1].exc_text is None
