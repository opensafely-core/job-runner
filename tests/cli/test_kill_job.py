from unittest import mock

import pytest

from jobrunner.cli import kill_job
from jobrunner.executors import local
from jobrunner.lib import database
from jobrunner.models import Job, State, StatusCode
from tests.factories import job_factory


@pytest.mark.parametrize("cleanup", [False, True])
def test_kill_job(cleanup, tmp_work_dir, db, monkeypatch):

    job = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)

    mocker = mock.MagicMock(spec=local.docker)
    mockume_api = mock.MagicMock(spec=local.volume_api)

    def mock_get_jobs(partial_job_ids):
        return [job]

    # persist_outputs needs this
    mocker.container_inspect.return_value = {
        "Image": "image",
        "State": {"ExitCode": 137},
    }

    # set both the docker module names used to the mocker version
    monkeypatch.setattr(kill_job, "docker", mocker)
    monkeypatch.setattr(local, "docker", mocker)
    monkeypatch.setattr(kill_job.local, "volume_api", mockume_api)
    monkeypatch.setattr(kill_job, "get_jobs", mock_get_jobs)

    kill_job.main(job.id, cleanup=cleanup)

    job1 = database.find_one(Job, id=job.id)
    assert job1.state == State.FAILED
    assert job1.status_code == StatusCode.KILLED_BY_ADMIN

    container = local.container_name(job)
    assert mocker.kill.call_args[0] == (container,)
    assert mocker.write_logs_to_file.call_args[0][0] == container

    log_dir = local.get_log_dir(job)
    log_file = log_dir / "logs.txt"
    metadata_file = log_dir / "metadata.json"

    assert log_file.exists()
    assert metadata_file.exists()

    workspace_log_file = (
        local.get_high_privacy_workspace(job.workspace)
        / local.METADATA_DIR
        / f"{job.action}.log"
    )
    assert not workspace_log_file.exists()

    if cleanup:
        assert mocker.delete_container.called
        assert mockume_api.delete_volume.called
    else:
        assert not mocker.delete_container.called
        assert not mockume_api.delete_volume.called
