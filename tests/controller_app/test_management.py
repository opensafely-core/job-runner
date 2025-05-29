"""
These management commands are wrappers around jobrunner/cli/controller and
core functionality is tested in tests/cli/controller; these tests just ensure
that they run correctly via management command.
"""

from django.core.management import call_command

from jobrunner.lib import database
from jobrunner.models import Job


def test_add_job(monkeypatch, tmp_work_dir, db, test_repo):
    assert not database.exists_where(Job)
    call_command("add_job", str(test_repo.path), "generate_dataset", backend="test")

    db_jobs = database.find_all(Job)
    assert len(db_jobs) == 1
    assert db_jobs[0].action == "generate_dataset"
