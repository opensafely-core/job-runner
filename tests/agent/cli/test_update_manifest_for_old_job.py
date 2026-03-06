import time
from unittest.mock import patch

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


def write_log_file(job_definition, level4_excluded_files=None):
    # write a mock log file with the metadata that update_manifest_for_old_job needs
    job_metadata_defaults = {key: "" for key in local.KEYS_TO_LOG}
    job_metadata = {
        **job_metadata_defaults,
        "job_definition_id": job_definition.id,
        "job_definition_rap_id": job_definition.rap_id,
        "user": job_definition.user,
        "level4_excluded_files": level4_excluded_files or {},
        "container_metadata": {
            "Config": {
                "Labels": {
                    "workspace": job_definition.workspace,
                    "action": job_definition.action,
                },
            }
        },
        "commit": job_definition.commit,
        "outputs": job_definition.output_spec,
    }
    with patch("agent.executors.local.docker.write_logs_to_file"):
        local.write_job_logs(job_definition, job_metadata)


def write_mock_manifest_file(job_definition, workspace_dir, outputs=None):
    # write a mock manifest file with one dummy output (which will be
    # used to retrieve the repo) and optional additional outputs
    outputs = outputs or {}
    manifest = {"outputs": {"dummy.txt": {"repo": job_definition.repo_url}, **outputs}}

    local.write_manifest_file(workspace_dir, manifest)


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


def test_update_manifest_for_old_job_with_excluded_file(job_definition, tmp_work_dir):
    # Set up manifest and log files for a previous job for an action that exists in the project.yaml,
    # which produced an excluded file
    job_definition.action = "generate_dataset"
    job_definition.output_spec = {
        "output/output.csv": "moderately_sensitive",
    }

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)
    # Write the message file for the too big CSV output file, it'll be used for file size etc
    (level4_dir / "output").mkdir(parents=True)
    (level4_dir / "output/output.csv.txt").write_text(
        "File row count (11) exceeds maximum allowed rows (10)"
    )

    # write mock manifest file (does not contain the output)
    write_mock_manifest_file(job_definition, level4_dir)
    # write the log file for the old job
    write_log_file(
        job_definition,
        level4_excluded_files={
            "output/output.csv": "File row count (11) exceeds maximum allowed rows (10)"
        },
    )

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


def test_update_manifest_does_not_overwrite_manifest_outputs(
    job_definition, tmp_work_dir
):
    # Set up manifest and log files for a previous job
    job_definition.action = "generate_dataset"
    job_definition.output_spec = {
        "output/output.csv": "moderately_sensitive",
    }

    level4_dir = local.get_medium_privacy_workspace(job_definition.workspace)

    # write mock manifest file (contains the output)
    write_mock_manifest_file(
        job_definition, level4_dir, outputs={"output/output.csv": {}}
    )
    # write the log file for the job
    write_log_file(job_definition)

    # try to update the manifest with this job
    update_manifest_for_old_job.run(
        [
            job_definition.workspace,
            job_definition.id,
            "main",
        ]
    )

    updated_manifest = local.read_manifest_file(level4_dir, job_definition)
    # The output exists in the manifest already, so is not marked as out of date
    output_metadata = updated_manifest["outputs"]["output/output.csv"]
    assert "out_of_date_output" not in output_metadata
    assert "out_of_date_action" not in output_metadata


def test_update_manifest_no_matching_manifest(tmp_work_dir):
    # try to update the manifest with this job
    with pytest.raises(AssertionError, match="Could not find existing manifest file"):
        update_manifest_for_old_job.run(
            [
                "workspace",
                "unknown_job_id",
                "main",
            ]
        )


def test_update_manifest_no_matching_job(tmp_work_dir):
    level4_dir = local.get_medium_privacy_workspace("workspace")
    local.write_manifest_file(level4_dir, {})
    assert (
        update_manifest_for_old_job.main(
            "workspace",
            "unknown_job_id",
            "main",
        )
        is None
    )


def test_get_full_job_id_no_matching_jobs(tmp_work_dir):
    assert update_manifest_for_old_job.get_full_job_id("foo") is None


def test_get_full_job_id_multiple_matching_jobs(tmp_work_dir, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "1")
    log_dir = tmp_work_dir / "job_log_dir"
    for job_id in ["os-job-1234566", "os-job-1234567"]:
        (log_dir / "2026-03" / job_id).mkdir(parents=True)
    assert update_manifest_for_old_job.get_full_job_id("123456") == "1234566"
