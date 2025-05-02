import json
import logging
import time
from unittest import mock

import pytest

from jobrunner import models, record_stats
from jobrunner.config import agent as config
from jobrunner.executors import local, volumes
from jobrunner.job_executor import ExecutorState, JobDefinition, Privacy, Study
from jobrunner.lib import datestr_to_ns_timestamp, docker
from tests.factories import ensure_docker_images_present, job_factory


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
        level4_max_filesize=16 * 1024 * 1024,
        level4_max_csv_rows=5000,
        level4_file_types=[".txt", ".csv"],
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
def get_docker_log(job_definition):
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


def test_read_metadata_path(job_definition):
    assert local.read_job_metadata(job_definition) == {}

    globbed_path = (
        config.JOB_LOG_DIR
        / "last-month"
        / local.container_name(job_definition)
        / local.METADATA_FILE
    )
    globbed_path.parent.mkdir(parents=True)
    globbed_path.write_text(json.dumps({"test": "globbed"}))
    assert local.read_job_metadata(job_definition) == local.METADATA_DEFAULTS | {
        "test": "globbed"
    }

    actual_path = local.get_log_dir(job_definition) / local.METADATA_FILE
    actual_path.parent.mkdir(parents=True)
    actual_path.write_text(json.dumps({"test": "actual"}))
    assert local.read_job_metadata(job_definition) == local.METADATA_DEFAULTS | {
        "test": "actual"
    }


@pytest.mark.needs_docker
def test_prepare_success(
    docker_cleanup, job_definition, test_repo, tmp_work_dir, freezer
):
    job_definition.inputs = ["output/input.csv"]
    populate_workspace(job_definition.workspace, "output/input.csv")

    expected_timestamp = time.time_ns()

    api = local.LocalDockerAPI()
    api.prepare(job_definition)
    status = api.get_status(job_definition)

    assert status.state == ExecutorState.PREPARED

    # we don't need to wait for this is currently synchronous
    next_status = api.get_status(job_definition)

    assert next_status.state == ExecutorState.PREPARED
    assert next_status.timestamp_ns == expected_timestamp

    assert volumes.volume_exists(job_definition)

    # check files have been copied
    expected = set(list_repo_files(test_repo.source) + job_definition.inputs)
    expected.add(local.TIMESTAMP_REFERENCE_FILE)

    # glob_volume_files uses find, and its '**/*' regex doesn't find files in
    # the root dir, which is arguably correct.
    files = volumes.glob_volume_files(job_definition)
    all_files = set(files["*"] + files["**/*"])
    assert all_files == expected


@pytest.mark.needs_docker
def test_prepare_already_prepared(docker_cleanup, job_definition):
    # create the volume already
    volumes.create_volume(job_definition)
    volumes.write_timestamp(job_definition, local.TIMESTAMP_REFERENCE_FILE)

    api = local.LocalDockerAPI()
    api.prepare(job_definition)
    status = api.get_status(job_definition)

    assert status.state == ExecutorState.PREPARED


@pytest.mark.needs_docker
def test_prepare_volume_exists_unprepared(docker_cleanup, job_definition):
    # create the volume already
    volumes.create_volume(job_definition)

    # do not write the timestamp, so prepare will rerun

    api = local.LocalDockerAPI()
    api.prepare(job_definition)
    status = api.get_status(job_definition)

    assert status.state == ExecutorState.PREPARED


@pytest.mark.needs_docker
def test_prepare_no_image(docker_cleanup, job_definition):
    job_definition.image = "invalid-test-image"
    api = local.LocalDockerAPI()

    with pytest.raises(local.LocalExecutorError) as exc:
        api.prepare(job_definition)

    assert job_definition.image in str(exc)


@pytest.mark.needs_docker
@pytest.mark.parametrize("ext", config.ARCHIVE_FORMATS)
def test_prepare_archived(ext, job_definition):
    api = local.LocalDockerAPI()
    archive = (config.HIGH_PRIVACY_ARCHIVE_DIR / job_definition.workspace).with_suffix(
        ext
    )
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.write_text("I exist")

    with pytest.raises(local.LocalExecutorError) as exc:
        api.prepare(job_definition)

    assert "has been archived" in str(exc)


@pytest.mark.needs_docker
def test_prepare_job_bad_commit(docker_cleanup, job_definition, test_repo):
    job_definition.study = Study(git_repo_url=str(test_repo.path), commit="bad-commit")

    with pytest.raises(local.LocalDockerError) as exc_info:
        local.prepare_job(job_definition)

    assert job_definition.study.commit in str(exc_info.value)


@pytest.mark.needs_docker
def test_prepare_job_no_input_file(docker_cleanup, job_definition):
    job_definition.inputs = ["output/input.csv"]

    with pytest.raises(local.LocalDockerError) as exc_info:
        local.prepare_job(job_definition)

    assert "output/input.csv" in str(exc_info.value)


@pytest.mark.needs_docker
def test_execute_success(docker_cleanup, job_definition, tmp_work_dir, db):
    # check limits are applied
    job_definition.cpu_count = 1.5
    job_definition.memory_limit = "1G"

    api = local.LocalDockerAPI()

    # use prepare step as test set up
    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED

    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    container_data = docker.container_inspect(local.container_name(job_definition))
    assert container_data["State"]["ExitCode"] == 0
    assert container_data["HostConfig"]["NanoCpus"] == int(1.5 * 1e9)
    assert container_data["HostConfig"]["Memory"] == 2**30  # 1G


@pytest.mark.needs_docker
def test_execute_metrics(docker_cleanup, job_definition, tmp_work_dir, db):
    job_definition.args = ["sleep", "10"]
    last_run = time.time_ns()

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED

    # we need scheduler job state to be able to collect stats
    job = job_factory(
        id=job_definition.id,
        state=models.State.RUNNING,
        status_code=models.StatusCode.EXECUTING,
        started_at=int(last_run / 1e9),
    )

    api.execute(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.EXECUTING

    # simulate stats thread collecting stats
    record_stats.record_tick_trace(last_run, [job])

    docker.kill(local.container_name(job_definition))

    status = wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    assert list(status.metrics.keys()) == [
        "cpu_sample",
        "cpu_cumsum",
        "cpu_mean",
        "cpu_peak",
        "mem_mb_sample",
        "mem_mb_cumsum",
        "mem_mb_mean",
        "mem_mb_peak",
        "container_id",
    ]


@pytest.mark.needs_docker
def test_execute_user_bindmount(docker_cleanup, job_definition, tmp_work_dir):
    api = local.LocalDockerAPI()
    # use prepare step as test set up
    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED

    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    container_config = docker.container_inspect(local.container_name(job_definition))

    # do not test that this config is set on platforms that do not require this config
    if config.DOCKER_USER_ID and config.DOCKER_GROUP_ID:
        assert (
            container_config["Config"]["User"]
            == f"{config.DOCKER_USER_ID}:{config.DOCKER_GROUP_ID}"
        )
    assert container_config["State"]["ExitCode"] == 0


@pytest.mark.needs_docker
def test_execute_not_prepared(docker_cleanup, job_definition, tmp_work_dir):
    api = local.LocalDockerAPI()

    api.execute(job_definition)
    status = api.get_status(job_definition)
    # this will be turned into an error by the loop
    assert status.state == ExecutorState.UNKNOWN


@pytest.mark.needs_docker
def test_finalize_success(docker_cleanup, job_definition, tmp_work_dir):
    job_definition.args = [
        "touch",
        "/workspace/output/output.csv",
        "/workspace/output/summary.csv",
        "/workspace/output/summary.txt",
    ]
    job_definition.inputs = ["output/input.csv"]
    job_definition.output_spec = {
        "output/output.*": "highly_sensitive",
        "output/summary.*": "moderately_sensitive",
    }
    populate_workspace(job_definition.workspace, "output/input.csv")

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    status = wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    # check that timestamp is as expected
    container = docker.container_inspect(local.container_name(job_definition))
    assert status.timestamp_ns == datestr_to_ns_timestamp(
        container["State"]["FinishedAt"]
    )

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    # for test debugging if any asserts fail
    print(get_docker_log(job_definition))
    assert status.results["exit_code"] == "0"
    assert status.results["outputs"] == {
        "output/output.csv": "highly_sensitive",
        "output/summary.csv": "moderately_sensitive",
        "output/summary.txt": "moderately_sensitive",
    }
    assert status.results["unmatched_patterns"] == []
    assert status.results["status_message"] == "Completed successfully"

    log_dir = local.get_log_dir(job_definition)
    log_file = log_dir / "logs.txt"
    assert log_dir.exists()
    assert log_file.exists()

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)
    manifest = local.read_manifest_file(level4_dir, job_definition)

    csv_metadata = manifest["outputs"]["output/summary.csv"]
    assert csv_metadata["level"] == "moderately_sensitive"
    assert csv_metadata["job_id"] == job_definition.id
    assert csv_metadata["job_request"] == job_definition.job_request_id
    assert csv_metadata["action"] == job_definition.action
    assert csv_metadata["commit"] == job_definition.study.commit
    assert csv_metadata["excluded"] is False
    assert csv_metadata["size"] == 0
    assert (
        csv_metadata["content_hash"]
        == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
    assert csv_metadata["row_count"] == 0
    assert csv_metadata["col_count"] == 0

    txt_metadata = manifest["outputs"]["output/summary.txt"]
    assert txt_metadata["excluded"] is False
    assert txt_metadata["row_count"] is None
    assert txt_metadata["col_count"] is None

    job_metadata = local.read_job_metadata(job_definition)
    for key in {
        "exit_code",
        "completed_at",
        "commit",
        "docker_image_id",
        "status_message",
        "outputs",
        "job_definition_id",
        "job_definition_request_id",
        "timestamp_ns",
    }:
        assert key in job_metadata.keys()


@pytest.mark.needs_docker
def test_finalize_failed(docker_cleanup, job_definition, tmp_work_dir):
    job_definition.args = ["false"]
    job_definition.output_spec = {
        "output/output.*": "highly_sensitive",
        "output/summary.*": "moderately_sensitive",
    }

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    # for test debugging if any asserts fail
    print(get_docker_log(job_definition))
    assert status.results["exit_code"] == "1"
    assert status.results["outputs"] == {}
    assert status.results["unmatched_patterns"] == [
        "output/output.*",
        "output/summary.*",
    ]


@pytest.mark.needs_docker
def test_finalize_no_container_metadata(monkeypatch, job_definition, tmp_work_dir):
    mocker = mock.MagicMock(spec=local.docker)
    mocker.container_inspect.return_value = {}

    job_definition.args = ["false"]
    job_definition.output_spec = {
        "output/output.*": "highly_sensitive",
        "output/summary.*": "moderately_sensitive",
    }

    api = local.LocalDockerAPI()

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.UNKNOWN


@pytest.mark.needs_docker
def test_finalize_unmatched(docker_cleanup, job_definition, tmp_work_dir):
    # the sleep is needed to make sure the unmatched file is *newer* enough
    job_definition.args = ["sh", "-c", "sleep 1; touch /workspace/unmatched"]
    job_definition.output_spec = {
        "output/output.*": "highly_sensitive",
        "output/summary.*": "moderately_sensitive",
    }

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    # for test debugging if any asserts fail
    print(get_docker_log(job_definition))
    assert status.results["exit_code"] == "0"
    assert status.results["outputs"] == {}
    assert status.results["unmatched_patterns"] == [
        "output/output.*",
        "output/summary.*",
    ]
    assert status.results["unmatched_outputs"] == ["unmatched"]
    assert status.results[
        "status_message"
    ] == "\n  No outputs found matching patterns:\n - {}".format(
        "\n   - ".join(["output/output.*", "output/summary.*"])
    )
    assert status.results[
        "hint"
    ] == "\n  Did you mean to match one of these files instead?\n - {}".format(
        "\n   - ".join(["unmatched"])
    )


@pytest.mark.needs_docker
def test_finalize_unmatched_output(docker_cleanup, job_definition, tmp_work_dir):
    # the sleep is needed to make sure the unmatched file is *newer* enough
    job_definition.args = ["sh", "-c", "sleep 1; touch /workspace"]
    job_definition.output_spec = {
        "output/output.*": "highly_sensitive",
        "output/summary.*": "moderately_sensitive",
    }

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    # for test debugging if any asserts fail
    print(get_docker_log(job_definition))
    assert status.results["exit_code"] == "0"
    assert status.results["outputs"] == {}
    assert status.results["unmatched_patterns"] == [
        "output/output.*",
        "output/summary.*",
    ]
    assert status.results["unmatched_outputs"] == []
    assert status.results[
        "status_message"
    ] == "\n  No outputs found matching patterns:\n - {}".format(
        "\n   - ".join(["output/output.*", "output/summary.*"])
    )


@pytest.mark.needs_docker
def test_finalize_failed_137(docker_cleanup, job_definition, tmp_work_dir):
    job_definition.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.EXECUTING

    # impersonate an admin
    docker.kill(local.container_name(job_definition))

    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert status.results["exit_code"] == "137"
    assert (
        status.results["status_message"]
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

    api.prepare(job_definition)
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert status.results["exit_code"] == "137"
    # Note, 6MB is rounded to 0.01GBM by the formatter
    assert (
        status.results["status_message"] == "Job ran out of memory (limit was 0.01GB)"
    )

    assert log_dir_log_file_exists(job_definition)
    assert workspace_log_file_exists(job_definition)


@pytest.mark.needs_docker
def test_finalize_large_level4_outputs(docker_cleanup, job_definition, tmp_work_dir):
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

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert status.results["exit_code"] == "0"
    assert status.results["level4_excluded_files"] == {
        "output/output.txt": "File size of 1.0Mb is larger that limit of 0.5Mb.",
    }

    log_file = local.get_log_dir(job_definition) / "logs.txt"
    log = log_file.read_text()
    assert "Invalid moderately_sensitive outputs:" in log
    assert (
        "output/output.txt  - File size of 1.0Mb is larger that limit of 0.5Mb." in log
    )

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)

    message_file = level4_dir / "output/output.txt.txt"
    txt = message_file.read_text()
    assert "output/output.txt" in txt
    assert "1.0Mb" in txt
    assert "0.5Mb" in txt

    manifest = local.read_manifest_file(level4_dir, job_definition)

    assert manifest["outputs"]["output/output.txt"]["excluded"]
    assert (
        manifest["outputs"]["output/output.txt"]["message"]
        == "File size of 1.0Mb is larger that limit of 0.5Mb."
    )


@pytest.mark.needs_docker
def test_finalize_invalid_file_type(docker_cleanup, job_definition, tmp_work_dir):
    job_definition.args = ["touch", "/workspace/output/output.rds"]
    job_definition.output_spec = {
        "output/output.rds": "moderately_sensitive",
    }
    job_definition.level4_file_types = [".csv"]

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert status.results["exit_code"] == "0"
    assert status.results["level4_excluded_files"] == {
        "output/output.rds": "File type of .rds is not valid level 4 file",
    }

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)
    message_file = level4_dir / "output/output.rds.txt"
    txt = message_file.read_text()
    assert "output/output.rds" in txt

    log_file = local.get_log_dir(job_definition) / "logs.txt"
    log = log_file.read_text()
    assert "Invalid moderately_sensitive outputs:" in log
    assert "output/output.rds  - File type of .rds is not valid level 4 file" in log

    manifest = local.read_manifest_file(level4_dir, job_definition)

    assert manifest["outputs"]["output/output.rds"]["excluded"]
    assert (
        manifest["outputs"]["output/output.rds"]["message"]
        == "File type of .rds is not valid level 4 file"
    )


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

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert status.results["exit_code"] == "0"
    assert status.results["level4_excluded_files"] == {
        "output/output.csv": "File has patient_id column",
    }

    log_file = local.get_log_dir(job_definition) / "logs.txt"
    log = log_file.read_text()
    assert "Invalid moderately_sensitive outputs:" in log
    assert "output/output.csv  - File has patient_id column" in log

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)

    message_file = level4_dir / "output/output.csv.txt"
    txt = message_file.read_text()
    assert "output/output.csv" in txt
    assert "patient_id" in txt

    manifest = local.read_manifest_file(level4_dir, job_definition)

    assert manifest["outputs"]["output/output.csv"]["excluded"]
    assert (
        manifest["outputs"]["output/output.csv"]["message"]
        == "File has patient_id column"
    )
    assert manifest["outputs"]["output/output.csv"]["row_count"] == 1
    assert manifest["outputs"]["output/output.csv"]["col_count"] == 3


@pytest.mark.needs_docker
def test_finalize_csv_max_rows(docker_cleanup, job_definition, tmp_work_dir):
    rows = "1,2\n" * 11
    job_definition.args = [
        "sh",
        "-c",
        f"echo 'foo,bar\n{rows}' > /workspace/output/output.csv",
    ]
    job_definition.output_spec = {
        "output/output.csv": "moderately_sensitive",
    }
    job_definition.level4_max_csv_rows = 10

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert status.results["exit_code"] == "0"
    assert status.results["level4_excluded_files"] == {
        "output/output.csv": "File row count (11) exceeds maximum allowed rows (10)",
    }

    log_file = local.get_log_dir(job_definition) / "logs.txt"
    log = log_file.read_text()
    assert "Invalid moderately_sensitive outputs:" in log
    assert (
        "output/output.csv  - File row count (11) exceeds maximum allowed rows (10)"
        in log
    )

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)

    message_file = level4_dir / "output/output.csv.txt"
    txt = message_file.read_text()
    assert "output/output.csv" in txt
    assert "contained 11 rows" in txt

    manifest = local.read_manifest_file(level4_dir, job_definition)

    assert manifest["outputs"]["output/output.csv"]["excluded"]
    assert (
        manifest["outputs"]["output/output.csv"]["message"]
        == "File row count (11) exceeds maximum allowed rows (10)"
    )

    assert manifest["outputs"]["output/output.csv"]["row_count"] == 11
    assert manifest["outputs"]["output/output.csv"]["col_count"] == 2


@pytest.mark.needs_docker
def test_finalize_large_level4_outputs_cleanup(
    docker_cleanup, job_definition, tmp_work_dir
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
    message_file.write_text("status_message")

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert status.results["exit_code"] == "0"
    assert status.results["level4_excluded_files"] == {}
    assert not message_file.exists()


@pytest.mark.needs_docker
def test_finalize_already_finalized_idempotent(
    job_definition, docker_cleanup, tmp_work_dir
):
    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)
    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED
    # check persistance and idempotence. if finalize actually called
    # finalize_job, we would expect an assertion error here
    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED


@pytest.mark.needs_docker
def test_finalize_already_finalized_with_error_idempotent(
    job_definition, docker_cleanup, tmp_work_dir
):
    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)
    api.finalize(job_definition, error={"test": "foo"})
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}
    assert status.results["exit_code"] == "0"

    # check persistance and idempotence. if finalize actually called
    # finalize_job, we would expect an assertion error here
    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}
    assert status.results["exit_code"] == "0"


@pytest.mark.needs_docker
def test_finalize_with_error_when_unknown(job_definition, docker_cleanup, tmp_work_dir):
    api = local.LocalDockerAPI()
    api.finalize(job_definition, error={"test": "foo"})
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}
    assert status.results["exit_code"] == "None"

    # check persistant
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}
    assert status.results["exit_code"] == "None"


@pytest.mark.needs_docker
def test_finalize_with_error_when_prepared(
    job_definition, docker_cleanup, tmp_work_dir
):
    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED

    api.finalize(job_definition, error={"test": "foo"})
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}
    assert status.results["exit_code"] == "None"

    # check persistant
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}
    assert status.results["exit_code"] == "None"


@pytest.mark.needs_docker
def test_finalize_with_error_when_executing(
    job_definition, docker_cleanup, tmp_work_dir
):
    job_definition.args = ["sleep", "101"]
    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.EXECUTING

    api.finalize(job_definition, error={"test": "foo"})
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}
    assert status.results["exit_code"] == "0"

    # check persistant
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}


@pytest.mark.needs_docker
def test_finalize_with_error_when_executed(
    job_definition, docker_cleanup, tmp_work_dir
):
    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition, error={"test": "foo"})
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}
    assert status.results["exit_code"] == "0"

    # check persistant
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.ERROR
    assert status.results["error"] == {"test": "foo"}


@pytest.mark.needs_docker
def test_pending_job_terminated_not_finalized(
    docker_cleanup, job_definition, tmp_work_dir
):
    job_definition.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    # user cancels the job before it's started
    api.terminate(job_definition)
    status = api.get_status(job_definition)
    job_definition.cancelled = "user"
    assert status.state == ExecutorState.UNKNOWN
    assert api.get_status(job_definition).state == ExecutorState.UNKNOWN

    assert not log_dir_log_file_exists(job_definition)
    assert not workspace_log_file_exists(job_definition)


@pytest.mark.needs_docker
def test_prepared_job_cancelled(docker_cleanup, job_definition, tmp_work_dir):
    job_definition.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED

    # Finalizing the job as cancelled sets cancelled metadata
    api.finalize(job_definition, cancelled=True)
    status = api.get_status(job_definition)
    assert status.results["cancelled"]
    assert status.state == ExecutorState.FINALIZED

    api.cleanup(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert not log_dir_log_file_exists(job_definition)
    assert not workspace_log_file_exists(job_definition)


@pytest.mark.needs_docker
def test_running_job_cancelled(docker_cleanup, job_definition, tmp_work_dir):
    job_definition.args = ["sleep", "101"]

    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED

    api.execute(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.EXECUTING

    api.terminate(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.EXECUTED

    api.finalize(job_definition, cancelled=True)
    status = api.get_status(job_definition)
    assert status.results["cancelled"]
    assert status.state == ExecutorState.FINALIZED
    assert status.results["exit_code"] == str(137)
    assert status.results["status_message"] == "Job cancelled by user"

    # Calling terminate again on a finalized job just returns the current status
    api.terminate(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    api.cleanup(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED

    assert log_dir_log_file_exists(job_definition)
    assert not workspace_log_file_exists(job_definition)


@pytest.mark.needs_docker
def test_cleanup_success(docker_cleanup, job_definition, tmp_work_dir):
    populate_workspace(job_definition.workspace, "output/input.csv")

    api = local.LocalDockerAPI()
    api.prepare(job_definition)
    api.execute(job_definition)

    container = local.container_name(job_definition)
    assert volumes.volume_exists(job_definition)
    assert docker.container_exists(container)

    api.cleanup(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.UNKNOWN

    assert not volumes.volume_exists(job_definition)
    assert not docker.container_exists(container)


def test_delete_files_success(tmp_work_dir):
    high = populate_workspace("test", "file.txt")
    medium = populate_workspace("test", "file.txt", privacy="medium")

    assert high.exists()
    assert medium.exists()

    api = local.LocalDockerAPI()
    api.delete_files("test", Privacy.HIGH, ["file.txt"])

    assert not high.exists()
    assert medium.exists()

    api.delete_files("test", Privacy.MEDIUM, ["file.txt"])

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
def test_write_read_timestamps(docker_cleanup, job_definition, tmp_work_dir):
    assert volumes.read_timestamp(job_definition, "test") is None

    volumes.create_volume(job_definition)
    before = time.time_ns()
    volumes.write_timestamp(job_definition, "test")
    after = time.time_ns()
    ts = volumes.read_timestamp(job_definition, "test")

    assert before <= ts <= after


@pytest.mark.needs_docker
def test_delete_volume(docker_cleanup, job_definition, tmp_work_dir):
    # check it doesn't error
    volumes.delete_volume(job_definition)

    # check it does remove volume
    volumes.create_volume(job_definition)
    volumes.write_timestamp(job_definition, local.TIMESTAMP_REFERENCE_FILE)

    volumes.delete_volume(job_definition)

    assert not volumes.volume_exists(job_definition)


@pytest.mark.needs_docker
def test_delete_volume_error_bindmount(
    docker_cleanup, job_definition, tmp_work_dir, monkeypatch, caplog
):
    def error(*args, **kwargs):
        raise Exception("some error")

    caplog.set_level(logging.ERROR)
    monkeypatch.setattr(volumes.shutil, "rmtree", error)

    volumes.delete_volume(job_definition)

    assert str(volumes.host_volume_path(job_definition)) in caplog.records[-1].msg
    assert "some error" in caplog.records[-1].exc_text


def test_delete_volume_error_file_bindmount_skips_and_logs(job_definition, caplog):
    caplog.set_level(logging.ERROR)

    # we can't easily manufacture a file permissions error, so we use
    # a different error to test our onerror handling code: directory does not
    # exist
    volumes.delete_volume(job_definition)

    # check the error is logged
    path = str(volumes.host_volume_path(job_definition))
    assert path in caplog.records[-1].msg
    # *not* an exception log, just an error one
    assert caplog.records[-1].exc_text is None


@pytest.mark.needs_docker
def test_finalize_job_with_error(job_definition):
    local.finalize_job(job_definition, error={"test": "foo"}, cancelled=False)
    metadata = local.read_job_metadata(job_definition)
    assert metadata["error"] == {"test": "foo"}
    assert metadata["status_message"] == "Job errored"
