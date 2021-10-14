from jobrunner.models import State
from jobrunner.queries import calculate_workspace_state
from tests.factories import job_factory


def test_gets_one_job(db):
    job_factory(workspace="the-workspace", action="the-action", state=State.SUCCEEDED)
    job = only(calculate_workspace_state("the-workspace"))
    assert job.action == "the-action"
    assert job.state == State.SUCCEEDED


def test_gets_a_job_for_each_action(db):
    job_factory(workspace="the-workspace", action="action1")
    job_factory(workspace="the-workspace", action="action2")
    jobs = calculate_workspace_state("the-workspace")
    assert len(jobs) == 2
    for action in ["action1", "action2"]:
        assert action in [job.action for job in jobs]


def test_gets_the_latest_job_for_an_action(db):
    job_factory(
        workspace="the-workspace",
        action="the-action",
        created_at=1000,
        state=State.FAILED,
    )
    job_factory(
        workspace="the-workspace",
        action="the-action",
        created_at=2000,
        state=State.SUCCEEDED,
    )
    job = only(calculate_workspace_state("the-workspace"))
    assert job.state == State.SUCCEEDED


def test_ignores_cancelled_jobs(db):
    job_factory(
        workspace="the-workspace",
        action="the-action",
        created_at=1000,
        state=State.FAILED,
    )
    job_factory(
        workspace="the-workspace",
        action="the-action",
        created_at=2000,
        state=State.SUCCEEDED,
        cancelled=True,
    )
    job = only(calculate_workspace_state("the-workspace"))
    assert job.state == State.FAILED


def test_doesnt_include_dummy_error_jobs(db):
    job_factory(workspace="the-workspace", action="__error__")
    jobs = calculate_workspace_state("the-workspace")
    assert not jobs


def only(xs):
    assert len(xs) == 1
    return xs[0]
