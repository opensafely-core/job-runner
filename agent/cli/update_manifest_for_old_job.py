"""
Update a manifest with outputs from a previously run job
Does not overwrite any outputs that already exist in the manifest
"""

import argparse
import shutil
import sys

from agent import config
from agent.cli.update_manifest_out_of_date_actions import PsuedoJobDefinition
from agent.executors.local import (
    MANIFEST_FILE,
    METADATA_DIR,
    container_name,
    get_csv_counts,
    get_medium_privacy_workspace,
    get_output_metadata,
    get_workspace_action_names,
    read_job_metadata,
    read_manifest_file,
    write_manifest_file,
)
from common.job_executor import Study
from common.lib.git import get_sha_from_remote_ref


def main(workspace, partial_job_id, branch):
    medium_privacy_dir = get_medium_privacy_workspace(workspace)
    # Retrieve existing manifest file and job_metadata from logs for this job
    manifest, job_metadata = get_existing_manifest_and_job_metadata(
        medium_privacy_dir, workspace, partial_job_id
    )
    if job_metadata is None:
        return

    # Find the repo_url from the first existing output in the manifest file
    repo_url = next(iter(manifest["outputs"].values()))["repo"]
    # Get current workspace actions so we can determine if this is an out-of-date action or an
    # out of date output
    commit = get_sha_from_remote_ref(repo_url, branch)
    job_definition = PsuedoJobDefinition("__none__", Study(repo_url, commit, branch))
    current_workspace_actions = get_workspace_action_names(job_definition)

    for output, level in job_metadata["outputs"].items():
        if output in manifest["outputs"]:
            print(f"{output} exists in manifest.json, skipping")
            continue
        print(f"Updating manifest output metadata for {output}")

        # Handle excluded files; these are replaced on disk (but not in the
        # manifest file) by a message file with a .txt extension
        excluded = output in job_metadata["level4_excluded_files"]
        if excluded:
            abspath = medium_privacy_dir / f"{output}.txt"
        else:
            abspath = medium_privacy_dir / output

        # Re-read CSV row/col counts as these are not stored in the job metadata
        csv_counts = {}
        if abspath.suffix == ".csv":
            try:
                csv_counts, _ = get_csv_counts(abspath)
            except Exception:  # pragma: no cover
                ...

        action = job_metadata["container_metadata"]["Config"]["Labels"]["action"]
        output_metadata = get_output_metadata(
            abspath=abspath,
            level=level,
            job_id=job_metadata["job_definition_id"],
            job_request=job_metadata["job_definition_rap_id"],
            action=action,
            commit=job_metadata["commit"],
            repo=repo_url,
            excluded=excluded,
            user=job_metadata["user"],
            message=job_metadata["level4_excluded_files"].get(output),
            csv_counts=csv_counts,
        )
        if action in current_workspace_actions:
            output_metadata["out_of_date_output"] = True
            output_metadata["out_of_date_action"] = False
        else:
            output_metadata["out_of_date_action"] = True
            output_metadata["out_of_date_output"] = False

        manifest["outputs"][output] = output_metadata

    write_manifest_file(medium_privacy_dir, manifest)


def get_existing_manifest_and_job_metadata(
    medium_privacy_dir, workspace, partial_job_id
):
    """
    Retrieve existing manifest file and job metadata, with some sanity checks to
    verify we've got the right ones for the workspace we said we're updating.
    """
    manifest_file = medium_privacy_dir / METADATA_DIR / MANIFEST_FILE
    # sanity check to confirm we didn't typo the workspace name and we have read a manifest file
    assert manifest_file.exists(), (
        f"Could not find existing manifest file for workspace {workspace}"
    )
    manifest = read_manifest_file(medium_privacy_dir, workspace)
    job_id = get_full_job_id(partial_job_id)
    if job_id is None:
        print(f"No match found for job id {partial_job_id}")
        return manifest, None

    job_metadata = read_job_metadata(job_id)
    # make sure this job metadata is for the workspace we expect
    workspace_from_job_metadata = job_metadata["container_metadata"]["Config"][
        "Labels"
    ]["workspace"]
    assert workspace_from_job_metadata == workspace, (
        f"Metadata for job {job_id} does not match workspace provided. Expected {workspace}, found {workspace_from_job_metadata}"
    )

    # backup manifest file, just in case.
    manifest_backup_file = manifest_file.parent / f"{manifest_file.name}{job_id}.bu"
    shutil.copy(manifest_file, manifest_backup_file)

    return manifest, job_metadata


def job_id_from_log_path(log_path):
    job_log_dir = log_path.stem
    return job_log_dir.lstrip("os-job-")


def get_full_job_id(partial_job_id):
    # sort for consistency in tests
    matching_job_ids = sorted(
        job_id_from_log_path(log_path)
        for log_path in config.JOB_LOG_DIR.glob(f"*/{container_name(partial_job_id)}*/")
    )
    if len(matching_job_ids) > 1:
        print(f"Multiple matches found for '{partial_job_id}':")
        for i, job_id in enumerate(matching_job_ids, start=1):
            print(f"  {i}: {job_id}")
        print()
        index = int(input("Enter number: "))
        assert 0 < index <= len(matching_job_ids)
        job = matching_job_ids[index - 1]
    elif len(matching_job_ids) == 1:
        job = matching_job_ids[0]
    else:
        job = None

    return job


def run(argv):
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument("workspace")
    parser.add_argument("partial_job_id", help="Full or partial job id")
    parser.add_argument("branch", help="workspace branch")
    args = parser.parse_args(argv)
    main(**vars(args))


if __name__ == "__main__":
    run(sys.argv[1:])
