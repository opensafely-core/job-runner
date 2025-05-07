from jobrunner.cli.controller import add_job
from jobrunner.lib import database
from jobrunner.models import Job


def test_add_job(monkeypatch, tmp_work_dir, db, test_repo):
    job_request, jobs = add_job.run([str(test_repo.path), "generate_dataset"])

    assert len(jobs) == 1
    assert jobs[0].action == "generate_dataset"

    db_jobs = database.find_where(Job, job_request_id=job_request.id)
    assert len(db_jobs) == 1
    assert db_jobs[0].action == "generate_dataset"


def test_add_job_with_bad_commit(monkeypatch, tmp_work_dir, db, test_repo):
    _, jobs = add_job.run([str(test_repo.path), "generate_dataset", "--commit", "abc"])

    assert len(jobs) == 1
    assert jobs[0].action == "__error__"
    assert "Could not find commit" in jobs[0].status_message
