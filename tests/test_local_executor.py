import pytest

from jobrunner.executors import local
from jobrunner.lib import docker
from jobrunner.job_executor import ExecutorState, JobStatus, JobResults, JobDefinition, Study
from jobrunner.run import job_to_job_definition
from jobrunner.manage_jobs import get_high_privacy_workspace
from jobrunner.models import State
from jobrunner import config


from tests.factories import ensure_docker_images_present



def populate_workspace(workspace, filename, content=None):
    path = get_high_privacy_workspace(workspace) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content or filename)


def list_repo_files(path):
    return list(str(f.relative_to(path)) for f in path.glob("**/*") if f.is_file())

@pytest.fixture
def use_api(monkeypatch):
    monkeypatch.setattr(config, "EXECUTION_API", True)


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
        inputs=["outputs/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    populate_workspace(job.workspace, "outputs/input.csv")

    api = local.LocalDockerJobAPI()
    status = api.prepare(job)

    assert status.state == ExecutorState.PREPARING

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
        inputs=["outputs/input.csv"],
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
        inputs=["outputs/input.csv"],
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
        inputs=["outputs/input.csv"],
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
        inputs=["outputs/input.csv"],
        output_spec={},
        allow_database_access=False,
    )

    with pytest.raises(local.LocalDockerError) as exc_info:
        local.prepare_job(job)

    assert "outputs/input.csv" in str(exc_info.value)


