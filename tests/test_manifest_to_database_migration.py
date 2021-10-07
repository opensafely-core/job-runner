import json
from collections import ChainMap

from jobrunner import config
from jobrunner.lib import database
from jobrunner.lib.database import insert
from jobrunner.manage_jobs import MANIFEST_FILE, METADATA_DIR
from jobrunner.manifest_to_database_migration import migrate_all, migrate_one
from jobrunner.models import Job, State, timestamp_to_isoformat


def test_migrates_a_workspace_with_one_action_and_one_output(tmp_work_dir):
    write_manifest(
        "the-workspace",
        "the-repo-url",
        actions_=actions(
            action(
                "the-job-id",
                "the-action",
                State.RUNNING,
                "the-commit",
                "the-image-id",
                1000000000,
                1000001000,
                [("the-file", "the-privacy-level")],
            )
        ),
    )

    migrate_all()

    assert_job_exists(
        workspace="the-workspace",
        repo_url="the-repo-url",
        job_id="the-job-id",
        action_="the-action",
        state=State.RUNNING,
        commit="the-commit",
        image_id="the-image-id",
        created_at=1000000000,
        completed_at=1000001000,
        outputs={"the-file": "the-privacy-level"},
    )


def test_migrates_a_job_with_multiple_outputs(tmp_work_dir):
    write_manifest(
        actions_=actions(
            action(
                job_id="the-job-id",
                outputs=[("file1", "level1"), ("file2", "level2")],
            )
        ),
    )

    migrate_all()

    assert_job_exists(
        job_id="the-job-id", outputs={"file1": "level1", "file2": "level2"}
    )


def test_migrates_a_job_with_no_outputs(tmp_work_dir):
    write_manifest(actions_=actions(action(job_id="the-job-id", outputs=[])))

    migrate_all()

    assert_job_exists(job_id="the-job-id", outputs={})


def test_copes_with_a_manifest_with_values_missing(tmp_work_dir):
    manifest = {
        "workspace": "the-workspace",
        "actions": {"the-action": {"job_id": "the-job-id"}},
    }
    manifest_file = (
        config.HIGH_PRIVACY_WORKSPACES_DIR
        / "the-workspace"
        / METADATA_DIR
        / MANIFEST_FILE
    )
    manifest_file.parent.mkdir(parents=True)
    manifest_file.write_text(json.dumps(manifest))

    migrate_all()

    assert_job_exists(
        job_id="the-job-id",
        workspace="the-workspace",
        action_="the-action",
        state=None,
        repo_url=None,
        commit=None,
        image_id=None,
        completed_at=0,
    )


def test_migrates_a_workspace_with_multiple_actions(tmp_work_dir):
    write_manifest(
        actions_=actions(
            action(
                action_="action1",
                job_id="job1",
                outputs=[("file1", "level1")],
            ),
            action(
                action_="action2",
                job_id="job2",
                outputs=[("file2", "level2")],
            ),
        ),
    )

    migrate_all()

    assert_job_exists(job_id="job1", action_="action1", outputs={"file1": "level1"})
    assert_job_exists(job_id="job2", action_="action2", outputs={"file2": "level2"})


def test_migrates_multiple_workspaces(tmp_work_dir):
    write_manifest(
        workspace="workspace1",
        repo_url="repo1",
        actions_=actions(action(job_id="job1")),
    )
    write_manifest(
        workspace="workspace2",
        repo_url="repo2",
        actions_=actions(action(job_id="job2")),
    )
    write_manifest(
        workspace="workspace3",
        repo_url="repo3",
        actions_=actions(action(job_id="job3")),
    )

    migrate_all()

    assert_job_exists(job_id="job1", workspace="workspace1", repo_url="repo1")
    assert_job_exists(job_id="job2", workspace="workspace2", repo_url="repo2")
    assert_job_exists(job_id="job3", workspace="workspace3", repo_url="repo3")


def test_migrates_a_single_one_of_several_workspaces(tmp_work_dir):
    write_manifest(
        workspace="workspace1",
        repo_url="repo1",
        actions_=actions(action(job_id="job1")),
    )
    write_manifest(
        workspace="workspace2",
        repo_url="repo2",
        actions_=actions(action(job_id="job2")),
    )

    migrate_one(config.HIGH_PRIVACY_WORKSPACES_DIR / "workspace1")

    assert_job_exists(job_id="job1", workspace="workspace1", repo_url="repo1")
    assert not database.find_where(Job, job_id="job2")


def test_migrates_jobs_in_batches(tmp_work_dir):
    for w in range(10):
        write_manifest(
            workspace=f"workspace-{w}",
            actions_=actions(
                *[
                    action(job_id=f"job-{w}-{j}", action_=f"action-{j}")
                    for j in range(10)
                ]
            ),
        )

    migrate_all(batch_size=5)
    assert len(database.find_all(Job)) == 5
    migrate_all(batch_size=10)
    assert len(database.find_all(Job)) == 15


def test_ignores_jobs_that_already_exist(tmp_work_dir):
    insert(job(job_id="the-job-id", state=State.PENDING))

    write_manifest(actions_=actions(action(job_id="the-job-id", state=State.RUNNING)))

    migrate_all()

    job_ = database.find_one(Job, id="the-job-id")
    assert job_.state == State.PENDING


def test_ignores_a_workspace_with_no_actions(tmp_work_dir):
    jobs_before = database.find_all(Job)
    write_manifest()

    migrate_all()

    jobs_after = database.find_all(Job)
    assert jobs_after == jobs_before


def test_ignores_a_directory_in_the_workspaces_dir_with_no_manifest(tmp_work_dir):
    workspace_dir = config.HIGH_PRIVACY_WORKSPACES_DIR / "the-workspace"
    workspace_dir.mkdir(parents=True)

    migrate_all()


def test_ignores_a_file_in_the_workspaces_dir(tmp_work_dir):
    errant_file = config.HIGH_PRIVACY_WORKSPACES_DIR / "some-errant-file"
    errant_file.parent.mkdir(parents=True)
    errant_file.touch()

    migrate_all()


def job(
    job_id=None,
    workspace="a-workspace",
    repo_url="a-repo-url",
    action_="an-action",
    state=State.PENDING,
    commit="a-commit",
    image_id="an-image-id",
    created_at=0,
    completed_at=1,
    outputs=None,
):
    assert job_id
    return Job(
        workspace=workspace,
        repo_url=repo_url,
        id=job_id,
        action=action_,
        state=state,
        commit=commit,
        image_id=image_id,
        created_at=created_at,
        completed_at=completed_at,
        outputs=outputs or {},
    )


def action(
    job_id=None,
    action_="an-action",
    state=State.PENDING,
    commit="a-commit",
    image_id="an-image-id",
    created_at=0,
    completed_at=1,
    outputs=None,
):
    assert job_id
    outputs = outputs or []

    job_ = {
        action_: {
            "job_id": job_id,
            "state": state.name,
            "commit": commit,
            "docker_image_id": image_id,
            "created_at": timestamp_to_isoformat(created_at),
            "completed_at": timestamp_to_isoformat(completed_at),
        }
    }

    files = {
        file: {"created_by_action": action_, "privacy_level": level}
        for file, level in outputs
    }

    return job_, files


def actions(*actions_):
    return zip(*actions_)


def write_manifest(
    workspace="a-workspace",
    repo_url="a-repo-url",
    actions_=None,
):
    jobs, outputs = actions_ or ([], [])

    manifest = {
        "workspace": workspace,
        "repo": repo_url,
        "actions": dict(ChainMap(*jobs)),
        "files": dict(ChainMap(*outputs)),
    }

    workspace_dir = config.HIGH_PRIVACY_WORKSPACES_DIR / workspace
    manifest_file = workspace_dir / METADATA_DIR / MANIFEST_FILE
    manifest_file.parent.mkdir(parents=True)
    manifest_file.write_text(json.dumps(manifest))


def assert_job_exists(job_id=None, **kwargs):
    all_jobs = database.find_all(Job)

    matching_jobs = database.find_where(Job, id=job_id)
    if not matching_jobs:
        raise AssertionError(f"Couldn't find job with id {job_id} but found {all_jobs}")
    if len(matching_jobs) > 1:
        raise AssertionError(
            f"Found more than one job with id {job_id}: {matching_jobs}"
        )
    single_job = matching_jobs[0]

    expected_job = job(job_id=job_id, **kwargs)
    if single_job != expected_job:
        raise AssertionError(f"Couldn't find {expected_job} amongst {all_jobs}")
