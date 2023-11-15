from jobrunner.cli import add_job
from jobrunner.lib import database
from jobrunner.models import Job


def test_add_job(tmp_work_dir, db, test_repo):
    job_request, jobs = add_job.run([str(test_repo.path), "generate_dataset"])

    assert len(jobs) == 1
    assert jobs[0].action == "generate_dataset"

    db_jobs = database.find_where(Job, job_request_id=job_request.id)
    assert len(db_jobs) == 1
    assert db_jobs[0].action == "generate_dataset"
