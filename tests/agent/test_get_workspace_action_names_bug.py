import pytest

from agent.executors.local import get_workspace_action_names
from controller.main import job_to_job_definition
from controller.models import Job, State
from controller.reusable_actions import (
    ReusableAction,
    handle_reusable_action,
    resolve_reusable_action_references,
)


workspace = {
    "repo_url": "https://github.com/opensafely/workspace-repo",
    "commit": "111111",
}

reusable_action_metadata = {
    "repo_url": "https://github.com/opensafely-actions/reusable",
    "commit": "222222",
}


@pytest.fixture
def jobs():
    job_template = {
        "state": State.PENDING,
        "workspace": "test-workspace",
        "backend": "test",
        "requires_outputs_from": [],
        "output_spec": {},
    } | workspace

    job_with_reusable_action = Job(
        id="job-1",
        rap_id="rap-1",
        action="use-reusable",
        run_command="reusable:v1 analysis/script.py",
        **job_template,
    )

    job_without_reusable_action = Job(
        id="job-2",
        rap_id="rap-2",
        action="regular-action",
        run_command="python:latest analysis/script.py",
        **job_template,
    )

    return [job_with_reusable_action, job_without_reusable_action]


@pytest.fixture
def mock_reusable_action():
    action = ReusableAction(
        repo_url=reusable_action_metadata["repo_url"],
        commit=reusable_action_metadata["commit"],
        action_file=b"run: python:latest some_script.py\n",
    )
    # Pre-populate the action run args to avoid parsing
    action._action_run_args = ["python:latest", "some_script.py"]
    return action


@pytest.fixture
def fake_github(mock_reusable_action):
    return {
        (workspace["repo_url"], workspace["commit"]): b"""
version: 4
actions:
  use-reusable:
    run: reusable:v1 analysis/script.py
    outputs:
      highly_sensitive:
        output: output/use-reusable.csv
  regular-action:
    run: python:latest analysis/script.py
    outputs:
      highly_sensitive:
        output: output/regular-action.csv

""",
        (mock_reusable_action.repo_url, mock_reusable_action.commit): b"""
version: 4
actions:
  some-other-action:
    run: python:v1 analysis/script.py
    outputs:
      highly_sensitive:
        data: output/some-other-action.csv
""",
    }


@pytest.mark.parametrize(
    "job_index, expected",
    [
        pytest.param(
            0,
            reusable_action_metadata,
            marks=pytest.mark.xfail(
                strict=True,
                reason="action_names is incorrectly the reusable action's actions, i.e. {'some-other-action'}",
            ),
        ),
        (1, workspace),
    ],
)
def test_get_workspace_action_names(
    monkeypatch, mock_reusable_action, jobs, fake_github, job_index, expected
):
    fake_cache = {
        ("reusable", "v1"): mock_reusable_action,
    }

    monkeypatch.setattr(
        "controller.reusable_actions.handle_reusable_action",
        lambda run_command, cache: handle_reusable_action(run_command, fake_cache),
    )
    resolve_reusable_action_references(jobs)

    monkeypatch.setattr(
        "controller.main.calculate_workspace_state",
        lambda backend, workspace: [],
    )

    job_definition = job_to_job_definition(jobs[job_index], "task-id", "image-sha")

    # These assertions demonstrate the reason for the bug and pass for both jobs
    assert job_definition.study.git_repo_url == expected.get("repo_url")
    assert job_definition.study.commit == expected.get("commit")

    monkeypatch.setattr(
        "agent.executors.local.read_file_from_repo",
        lambda repo_url, commit, path: fake_github.get((repo_url, commit)),
    )
    action_names = get_workspace_action_names(job_definition)

    # This assertion fails for job_index=0 (reusable action job)
    assert action_names == {"use-reusable", "regular-action"}
