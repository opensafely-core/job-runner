import pytest
import time
import subprocess

from jobrunner.executors import local
from jobrunner.lib import docker
from jobrunner.job_executor import (
    ExecutorState,
    JobStatus,
    JobResults,
    JobDefinition,
    Study,
)
from jobrunner.run import job_to_job_definition
from jobrunner.manage_jobs import get_high_privacy_workspace, container_name
from jobrunner.models import State
from jobrunner import config


from tests.factories import ensure_docker_images_present


def populate_workspace(workspace, filename, content=None):
    path = get_high_privacy_workspace(workspace) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content or filename)


# used for tests and debugging
def get_log(job):
    result = docker.docker(
        ["container", "logs", container_name(job)],
        check=True,
        capture_output=True,
    )
    return result.stdout + result.stderr


def wait_for_state(api, job, state, limit=5, step=0.25):
    """Utility to wait on a state change in the api."""
    start = time.time()
    elapsed = 0

    while api.get_status(job).state != state:
        elapsed = time.time() - start
        if elapsed > limit:
            raise Exception(f"Timed out waiting for state {state} for job {job}")

        time.sleep(step)


def list_repo_files(path):
    return list(str(f.relative_to(path)) for f in path.glob("**/*") if f.is_file())


@pytest.fixture
def use_api(monkeypatch):
    monkeypatch.setattr(config, "EXECUTION_API", True)
    yield
    local.RESULTS.clear()


@pytest.mark.needs_docker
def test_prepare_success(use_api, docker_cleanup, test_repo, tmp_work_dir):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test-id",
        study=test_repo.study,
        workspace="test",
        action="action",
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerJobAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.PREPARING

    # we don't need to wait for this is currently synchronous
    assert api.get_status(job).state == ExecutorState.PREPARED

    volume = local.volume_name(job)
    assert docker.volume_exists(volume)

    # check files have been copied
    expected = set(list_repo_files(test_repo.source) + job.inputs)
    expected.add(local.TIMESTAMP_REFERENCE_FILE)

    # glob_volume_files uses find, and its '**/*' regex doesn't find files in
    # the root dir, which is arguably correct.
    files = docker.glob_volume_files(volume, ["*", "**/*"])
    all_files = set(files["*"] + files["**/*"])
    assert all_files == expected


@pytest.mark.needs_docker
def test_prepare_already_prepared(use_api, docker_cleanup, test_repo):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_prepare_already_prepared",
        study=test_repo.study,
        workspace="test",
        action="action",
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    # create the volume already
    docker.create_volume(local.volume_name(job))

    api = local.LocalDockerJobAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.PREPARED


@pytest.mark.needs_docker
def test_prepare_no_image(use_api, docker_cleanup, test_repo):
    job = JobDefinition(
        id="test_prepare_no_image",
        study=test_repo.study,
        workspace="test",
        action="action",
        image="invalid-test-image",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    api = local.LocalDockerJobAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.ERROR
    assert job.image in status.message.lower()


@pytest.mark.needs_docker
def test_prepare_job_bad_commit(use_api, docker_cleanup, test_repo):
    job = JobDefinition(
        id="test_prepare_job_bad_commit",
        study=Study(git_repo_url=str(test_repo.path), commit="bad-commit"),
        workspace="test",
        action="action",
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
def test_prepare_job_no_input_file(use_api, docker_cleanup, test_repo):
    job = JobDefinition(
        id="test_prepare_job_no_input_file",
        study=test_repo.study,
        workspace="test",
        action="action",
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
def test_execute_success(use_api, docker_cleanup, test_repo, tmp_work_dir):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_execute_success",
        study=test_repo.study,
        workspace="test",
        action="action",
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerJobAPI()

    # use prepare step as test set up
    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARING

    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    # could be in either state
    assert api.get_status(job).state in (
        ExecutorState.EXECUTING,
        ExecutorState.EXECUTED,
    )


@pytest.mark.needs_docker
def test_execute_not_prepared(use_api, docker_cleanup, test_repo, tmp_work_dir):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_execute_not_prepared",
        study=test_repo.study,
        workspace="test",
        action="action",
        image="ghcr.io/opensafely-core/busybox",
        args=["/usr/bin/true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    api = local.LocalDockerJobAPI()

    status = api.execute(job)
    # this will be turned into an error by the loop
    assert status.state == ExecutorState.UNKNOWN


@pytest.mark.needs_docker
def test_finalize_success(use_api, docker_cleanup, test_repo, tmp_work_dir):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_finalize_finalized",
        study=test_repo.study,
        workspace="test",
        action="action",
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

    api = local.LocalDockerJobAPI()

    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARING
    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    wait_for_state(api, job, ExecutorState.EXECUTED)

    status = api.finalize(job)
    assert status.state == ExecutorState.FINALIZING

    # we don't need to wait
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
def test_finalize_failed(use_api, docker_cleanup, test_repo, tmp_work_dir):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_finalize_failed",
        study=test_repo.study,
        workspace="test",
        action="action",
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

    api = local.LocalDockerJobAPI()

    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARING
    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    wait_for_state(api, job, ExecutorState.EXECUTED)

    status = api.finalize(job)
    assert status.state == ExecutorState.FINALIZING

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
def test_finalize_unmatched(use_api, docker_cleanup, test_repo, tmp_work_dir):
    ensure_docker_images_present("busybox")

    job = JobDefinition(
        id="test_finalize_unmatched",
        study=test_repo.study,
        workspace="test",
        action="action",
        image="ghcr.io/opensafely-core/busybox",
        args=["true"],
        env={},
        inputs=["output/input.csv"],
        output_spec={
            "output/output.*": "high_privacy",
            "output/summary.*": "medium_privacy",
        },
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "output/input.csv")

    api = local.LocalDockerJobAPI()

    status = api.prepare(job)
    assert status.state == ExecutorState.PREPARING
    status = api.execute(job)
    assert status.state == ExecutorState.EXECUTING

    wait_for_state(api, job, ExecutorState.EXECUTED)

    status = api.finalize(job)
    assert status.state == ExecutorState.FINALIZING

    # we don't need to wait
    assert api.get_status(job).state == ExecutorState.FINALIZED
    assert job.id in local.RESULTS

    # for test debugging if any asserts fail
    print(get_log(job))
    results = api.get_results(job)
    assert results.exit_code == 0
    assert results.outputs == {}
    assert results.unmatched_patterns == ["output/output.*", "output/summary.*"]
