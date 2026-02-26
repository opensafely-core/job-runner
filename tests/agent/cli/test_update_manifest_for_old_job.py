import time
from copy import deepcopy

import pytest

from agent.cli import update_manifest_for_old_job
from agent.executors import local
from common.job_executor import ExecutorState, JobDefinition, Study
from tests.agent.test_local_executor import wait_for_state
from tests.factories import ensure_docker_images_present


@pytest.fixture()
def job_definition(request, test_repo, responses):
    """Basic simple action with no inputs as base for testing."""
    if "needs_docker" in list(m.name for m in request.node.iter_markers()):
        ensure_docker_images_present("busybox")

    responses.add_passthru("https://ghcr.io/")

    # replace parameterized tests [/] chars
    clean_name = request.node.name.replace("[", "_").replace("]", "_")
    return JobDefinition(
        id=clean_name,
        rap_id=f"job-request-{clean_name}",
        task_id=f"{clean_name}-001",
        study=Study(test_repo.repo_url, test_repo.commit, "main"),
        repo_url=str(test_repo.path),
        commit=test_repo.commit,
        workspace="test",
        action="action",
        created_at=int(time.time()),
        user="testuser",
        image="ghcr.io/opensafely-core/busybox:latest",
        image_sha=None,
        args=["true"],
        inputs=[],
        input_job_ids=[],
        env={},
        output_spec={},
        allow_database_access=False,
        level4_max_filesize=16 * 1024 * 1024,
        level4_max_csv_rows=5000,
        level4_file_types=[".txt", ".csv"],
    )


def run_job(job_definition):
    api = local.LocalDockerAPI()

    api.prepare(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.PREPARED
    api.execute(job_definition)
    wait_for_state(api, job_definition, ExecutorState.EXECUTED)

    api.finalize(job_definition)
    status = api.get_status(job_definition)
    assert status.state == ExecutorState.FINALIZED
    return status


@pytest.mark.parametrize(
    "action,is_current_action", [("generate_dataset", True), ("an_old_action", False)]
)
@pytest.mark.needs_docker
def test_update_manifest_for_old_job(
    docker_cleanup, job_definition, tmp_work_dir, action, is_current_action
):
    # Set up and run a previous job for the specified action
    job_definition.action = action
    rows = "1,2\n" * 11
    job_definition.args = [
        "sh",
        "-c",
        f"echo 'foo,bar\n{rows}' > /workspace/output/output.csv",
    ]
    job_definition.output_spec = {
        "output/output.csv": "moderately_sensitive",
    }
    run_job(job_definition)

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)
    # check manifest file
    manifest = local.read_manifest_file(level4_dir, job_definition)
    assert manifest["outputs"]["output/output.csv"]["row_count"] == 11
    assert manifest["outputs"]["output/output.csv"]["col_count"] == 2

    # update the outputs in the manifest file to include only a mock
    # previous job output with the repo url which we'll extract for the
    # updated output
    manifest["outputs"] = {"output/other.csv": {"repo": job_definition.repo_url}}
    local.write_manifest_file(level4_dir, manifest)

    # update the manifest with the old job metadata
    update_manifest_for_old_job.run(
        [
            job_definition.workspace,
            # pass just the first 5 characters of the job id
            job_definition.id[:5],
            "main",
        ]
    )

    updated_manifest = local.read_manifest_file(level4_dir, job_definition)
    output_metadata = updated_manifest["outputs"]["output/output.csv"]
    assert output_metadata["job_id"] == job_definition.id
    assert output_metadata["repo"] == job_definition.repo_url
    assert output_metadata["action"] == job_definition.action
    assert output_metadata["commit"] == job_definition.commit
    assert output_metadata["out_of_date_output"] == is_current_action
    assert output_metadata["out_of_date_action"] == (not is_current_action)
    assert output_metadata["row_count"] == 11
    assert output_metadata["col_count"] == 2


@pytest.mark.needs_docker
def test_update_manifest_for_old_job_with_excluded_file(
    docker_cleanup, job_definition, tmp_work_dir
):
    # Set up and run a previous job for an action that exists in the project.yaml, which
    # produced an excluded file
    job_definition.action = "generate_dataset"
    rows = "1,2\n" * 11
    job_definition.args = [
        "sh",
        "-c",
        f"echo 'foo,bar\n{rows}' > /workspace/output/output.csv",
    ]
    job_definition.output_spec = {
        "output/output.csv": "moderately_sensitive",
    }
    job_definition.level4_max_csv_rows = 10

    # Run job, confirm this file was excluded and written to a message file
    status = run_job(job_definition)
    assert status.results["level4_excluded_files"] == {
        "output/output.csv": "File row count (11) exceeds maximum allowed rows (10)",
    }

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)

    message_file = level4_dir / "output/output.csv.txt"
    txt = message_file.read_text()
    assert "output/output.csv" in txt
    assert "contained 11 rows" in txt

    # check manifest file
    manifest = local.read_manifest_file(level4_dir, job_definition)

    assert manifest["outputs"]["output/output.csv"]["excluded"]
    assert (
        manifest["outputs"]["output/output.csv"]["message"]
        == "File row count (11) exceeds maximum allowed rows (10)"
    )
    assert manifest["outputs"]["output/output.csv"]["row_count"] == 11
    assert manifest["outputs"]["output/output.csv"]["col_count"] == 2

    # update the outputs in the manifest file to include only a mock
    # previous job output with the repo url which we'll extract for the
    # updated output
    manifest["outputs"] = {"output/other.csv": {"repo": job_definition.repo_url}}
    # Add a mock existing output
    local.write_manifest_file(level4_dir, manifest)

    # update the manifest with the old job metadata
    update_manifest_for_old_job.run(
        [job_definition.workspace, job_definition.id, "main"]
    )

    updated_manifest = local.read_manifest_file(level4_dir, job_definition)
    output_metadata = updated_manifest["outputs"]["output/output.csv"]
    assert output_metadata["job_id"] == job_definition.id
    assert output_metadata["repo"] == job_definition.repo_url
    assert output_metadata["action"] == job_definition.action
    assert output_metadata["commit"] == job_definition.commit
    assert output_metadata["out_of_date_output"] is True
    assert output_metadata["out_of_date_action"] is False
    assert output_metadata["excluded"]
    assert (
        output_metadata["message"]
        == "File row count (11) exceeds maximum allowed rows (10)"
    )
    assert output_metadata["row_count"] is None
    assert output_metadata["col_count"] is None


@pytest.mark.needs_docker
def test_update_manifest_does_not_overwrite_manifest_outputs(
    docker_cleanup, job_definition, tmp_work_dir, capsys
):
    job_definition.args = [
        "sh",
        "-c",
        "echo 'foo' > /workspace/output/output.txt",
    ]
    job_definition.output_spec = {
        "output/output.txt": "moderately_sensitive",
    }

    run_job(job_definition)

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)

    # try to update the manifest with this job
    update_manifest_for_old_job.run(
        [
            job_definition.workspace,
            job_definition.id,
            "main",
        ]
    )

    updated_manifest = local.read_manifest_file(level4_dir, job_definition)
    output_metadata = updated_manifest["outputs"]["output/output.txt"]
    assert "out_of_date_output" not in output_metadata

    stdout = capsys.readouterr().out
    assert "output/output.txt exists in manifest.json, skipping" in stdout


def test_update_manifest_multiple_matching_jobs(
    docker_cleanup, job_definition, tmp_work_dir, monkeypatch
):
    monkeypatch.setattr("builtins.input", lambda _: "1")
    # Setup 2 jobs for the same action, writing to different output paths
    job_definition1 = deepcopy(job_definition)
    job_definition1.id = job_definition.id + "1"

    job_definition.args = [
        "sh",
        "-c",
        "echo 'foo' > /workspace/output/output.txt",
    ]
    job_definition.output_spec = {
        "output/output.txt": "moderately_sensitive",
    }
    job_definition1.args = [
        "sh",
        "-c",
        "echo 'bar' > /workspace/output/output1.txt",
    ]
    job_definition1.output_spec = {
        "output/output1.txt": "moderately_sensitive",
    }

    run_job(job_definition)
    run_job(job_definition1)

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)
    manifest = local.read_manifest_file(level4_dir, job_definition.workspace)
    # remove output from first job
    del manifest["outputs"]["output/output.txt"]
    local.write_manifest_file(level4_dir, manifest)

    # update the manifest with this job; the job id matches both jobs
    update_manifest_for_old_job.run(
        [
            job_definition.workspace,
            job_definition.id,
            "main",
        ]
    )

    updated_manifest = local.read_manifest_file(level4_dir, job_definition)
    output_metadata = updated_manifest["outputs"]["output/output.txt"]
    assert "out_of_date_output" in output_metadata


@pytest.mark.needs_docker
def test_update_manifest_no_matching_manifest(docker_cleanup, tmp_work_dir, capsys):
    # try to update the manifest with this job
    with pytest.raises(AssertionError, match="Could not find existing manifest file"):
        update_manifest_for_old_job.run(
            [
                "workspace",
                "unknown_job_id",
                "main",
            ]
        )


@pytest.mark.needs_docker
def test_update_manifest_no_matching_job(docker_cleanup, tmp_work_dir, capsys):
    level4_dir = local.get_medium_privacy_workspace("workspace")
    local.write_manifest_file(level4_dir, {})

    update_manifest_for_old_job.run(
        [
            "workspace",
            "unknown_job_id",
            "main",
        ]
    )
    stdout = capsys.readouterr().out
    assert "No match found for job id unknown_job_id" in stdout
