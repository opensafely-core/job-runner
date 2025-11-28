import pytest

from common.lib.github_validators import GithubValidationError
from controller.cli import add_job
from controller.lib import database
from controller.models import Job


def test_add_job(monkeypatch, tmp_work_dir, db, test_repo):
    rap_create_request, jobs = add_job.run(
        [str(test_repo.path), "generate_dataset", "--backend", "test"]
    )

    assert len(jobs) == 1
    assert jobs[0].action == "generate_dataset"

    db_jobs = database.find_where(Job, rap_id=rap_create_request.id)
    assert len(db_jobs) == 1
    assert db_jobs[0].action == "generate_dataset"


def test_add_job_with_bad_commit(monkeypatch, tmp_work_dir, db):
    with pytest.raises(GithubValidationError):
        add_job.run(
            [
                "https://github.com/opensafely/documentation",
                "generate_dataset",
                "--commit",
                "doesnotexist",
                "--backend",
                "test",
            ]
        )
