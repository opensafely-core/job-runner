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

        level4_dir = local.get_medium_privacy_workspace(workspace)

        if not level4_dir.exists():
            print(" - L4 dir doesn't exist")
            continue

        manifest_path = level4_dir / "metadata/manifest.json"

        if manifest_path.exists():
            update_manifest(workspace)
        else:
            # this was skipped in initial backfill, as L3 dir was archived
            # but we still need manifest.json for the L4 dir for airlock to
            # work. But its constructed differently - we'll only have l4 output
            # files available.
            write_archived_manifest(workspace)


LIMIT = 16 * 1024 * 1024


def update_manifest(workspace):
    """update repo and col/row counts, if missing."""
    level4_dir = local.get_medium_privacy_workspace(workspace)
    workspace_dir = local.get_high_privacy_workspace(workspace)
    manifest = local.read_manifest_file(level4_dir, workspace)

    for output, metadata in manifest["outputs"].items():
        if "repo" not in metadata:
            job = database.find_one(Job, id=metadata["job_id"])
            metadata["repo"] = job.repo_url
            print(f" - updating repo for {output}")

        if metadata["level"] == "moderately_sensitive" and output.endswith(".csv"):
            abspath = level4_dir / output
            if not abspath.exists():
                # excluded file, so look at L3 file
                abspath = workspace_dir / output

                if not abspath.exists():
                    print(f" - {output} does not exist any more")
                    continue

                if abspath.stat().st_size > LIMIT:
                    print(f" - {output} is too large to measure rows")
                    continue

            try:
                csv_counts, headers = local.get_csv_counts(abspath)
            except Exception:
                csv_counts = {}

            print(f" - updating row/col counts for {output}")
            metadata["row_count"] = csv_counts.get("rows")
            metadata["col_count"] = csv_counts.get("cols")

    print(
        f" - writing manifest for archived workspace {workspace} with {len(manifest['outputs'])} outputs"
    )
    local.write_manifest_file(level4_dir, manifest)


def write_archived_manifest(workspace):
    conn = database.get_connection()
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

            if level != "moderately_sensitive":
                continue

            # use presence of message file to detect excluded files
            abspath = level4_dir / output
            message_file = level4_dir / (output + ".txt")

            excluded = message_file.exists()

            if abspath.exists():
                csv_counts = {}
                if abspath.name.suffix == ".csv":
                    csv_counts = local.get_csv_counts()

                metadata = local.get_output_metadata(
                    abspath,
                    level,
                    job_id=job_id,
                    job_request=job.job_request_id,
                    action=job.action,
                    commit=job.commit,
                    repo=job.repo_url,
                    excluded=excluded,
                    csv_counts=csv_counts,
                )

            else:
                # we don't have the source file to hash or inspect, probably because it was excluded
                metadata = {
                    "level": level,
                    "job_id": job.id,
                    "job_request": job.job_request_id,
                    "action": job.action,
                    "commit": job.commit,
                    "repo": job.repo_url,
                    "size": 0,
                    "timestamp": message_file.stat().st_mtime if excluded else None,
                    "content_hash": None,
                    "excluded": excluded,
                    "message": None,
                    "row_count": None,
                    "col_count": None,
                }

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
