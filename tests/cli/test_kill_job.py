from unittest import mock

from jobrunner.cli import kill_job
from jobrunner.executors.local import volume_api
from jobrunner.lib import database, docker
from jobrunner.models import Job, State, StatusCode
from tests.factories import job_factory


def test_kill_job(db, monkeypatch):

    j1 = job_factory(state=State.RUNNING, status_code=StatusCode.EXECUTING)

    mocker = mock.MagicMock(spec=docker)
    mockume_api = mock.MagicMock(spec=volume_api)

    def mock_get_jobs(partial_job_ids):
        return [j1]

    monkeypatch.setattr(kill_job, "docker", mocker)
    monkeypatch.setattr(kill_job, "volume_api", mockume_api)
    monkeypatch.setattr(kill_job, "get_jobs", mock_get_jobs)

    kill_job.main(j1.id)

    job1 = database.find_one(Job, id=j1.id)
    assert job1.state == State.FAILED
    assert job1.status_code == StatusCode.KILLED_BY_ADMIN
