"""
Ops utility for backfilling manifest.json files from db
"""
import argparse

from jobrunner.executors import local
from jobrunner.lib import database
from jobrunner.models import Job


def main():
    conn = database.get_connection()

    workspaces = [
        w["workspace"] for w in conn.execute("SELECT DISTINCT(workspace) FROM job;")
    ]

    n_workspaces = len(workspaces)
    for i, workspace in enumerate(workspaces):
        print(f"workspace {i+1}/{n_workspaces}: {workspace}")

        workspace_dir = local.get_high_privacy_workspace(workspace)
        if not workspace_dir.exists():
            print(f" - workspace {workspace} is archived")
            continue

        level4_dir = local.get_medium_privacy_workspace(workspace)

        sentinel = level4_dir / ".manifest-backfill"
        if sentinel.exists():
            print(" - already done, skipping")
            continue

        write_manifest(workspace)

        sentinel.touch()


def write_manifest(workspace):
    conn = database.get_connection()
    workspace_dir = local.get_high_privacy_workspace(workspace)
    level4_dir = local.get_medium_privacy_workspace(workspace)

    # ordering by most recent ensures we find the job that generated the
    # current version of the file.
    job_ids = conn.execute(
        """
        SELECT id FROM job
        WHERE workspace = ?
            AND outputs != ''
            AND completed_at IS NOT NULL
            AND state = 'succeeded'
        ORDER BY completed_at DESC;
        """,
        (workspace,),
    )

    outputs = {}

    for row in job_ids:
        job_id = row["id"]
        job = database.find_one(Job, id=job_id)

        for output, level in job.outputs.items():
            if output in outputs:
                # older version of the file, ignore
                continue

            abspath = workspace_dir / output

            if not abspath.exists():
                print(f" - {output}, {level}: old output no longer on disk")
                continue

            # use presence of message file to detect excluded files
            message_file = level4_dir / (output + ".txt")
            excluded = message_file.exists()

            metadata = local.get_output_metadata(
                abspath,
                level,
                job_id=job_id,
                job_request=job.job_request_id,
                action=job.action,
                commit=job.commit,
                excluded=excluded,
            )

            outputs[output] = metadata

    manifest = local.read_manifest_file(level4_dir, workspace)
    manifest["outputs"] = outputs
    print(f" - writing manifest for {workspace} with {len(outputs)} outputs")
    local.write_manifest_file(level4_dir, manifest)


def run():
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    args = parser.parse_args()
    main(**vars(args))


if __name__ == "__main__":
    run()
