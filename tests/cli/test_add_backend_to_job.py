import pytest

from jobrunner.cli import add_backend_to_job
from jobrunner.lib import database
from jobrunner.models import Job, SavedJobRequest
from tests.factories import job_factory, job_request_factory, job_request_factory_raw


def test_add_backend_to_job(db, monkeypatch):
    monkeypatch.setattr("jobrunner.config.BACKEND", "dummy_backend")
    # job with no SavedJobRequest instance
    job1 = job_factory(job_request=job_request_factory_raw(), backend=None)
    assert not database.find_where(SavedJobRequest, id=job1.job_request_id)

    # job with job_request and SavedJobRequest instance
    job_request = job_request_factory(original={"backend": "the_test_backend"})
    job2 = job_factory(job_request, backend=None)
    assert database.find_where(SavedJobRequest, id=job2.job_request_id)

    # job with no job_request id
    job3 = job_factory(backend=None)
    job3.job_request_id = None
    database.update(job3)
    assert not database.find_where(SavedJobRequest, id=job3.job_request_id)
    assert database.find_one(Job, id=job3.id).job_request_id is None

    for job in [job1, job2, job3]:
        assert database.find_one(Job, id=job.id).backend is None, job

    add_backend_to_job.main()
    assert database.find_one(Job, id=job1.id).backend == "dummy_backend"
    assert database.find_one(Job, id=job2.id).backend == "the_test_backend"
    assert database.find_one(Job, id=job3.id).backend == "dummy_backend"


@pytest.mark.parametrize(
    "response,expected_backend",
    [
        ("Y", "dummy_backend"),
        ("y", "dummy_backend"),
        ("N", None),
        ("foo", None),
    ],
)
def test_add_backend_to_job_already_done(db, monkeypatch, response, expected_backend):
    monkeypatch.setattr("jobrunner.config.BACKEND", "dummy_backend")
    # job with a backend already set
    job1 = job_factory(backend="test")
    job2 = job_factory(backend=None)

    monkeypatch.setattr("builtins.input", lambda _: response)
    add_backend_to_job.main()
    assert database.find_one(Job, id=job1.id).backend == "test"
    assert database.find_one(Job, id=job2.id).backend == expected_backend
