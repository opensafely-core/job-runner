"""
Update a manifest to indicate actions that are out of date actions.

Reads the current manifest file, and updates it according
to the project.yaml retrieved for a specific commit in the
workspace repo.
"""

import argparse
import sys
from dataclasses import dataclass

from agent.executors.local import (
    get_medium_privacy_workspace,
    read_manifest_file,
    update_manifest_outputs_and_actions,
    write_manifest_file,
)
from common.job_executor import Study


@dataclass(frozen=True)
class PsuedoJobDefinition:
    action: str
    study: Study


def main(workspace, repo_url, commit):
    medium_privacy_dir = get_medium_privacy_workspace(workspace)
    manifest = read_manifest_file(medium_privacy_dir, workspace)
    job_definition = PsuedoJobDefinition("__none__", Study(repo_url, commit, "main"))
    update_manifest_outputs_and_actions(manifest, job_definition, new_outputs={})
    write_manifest_file(medium_privacy_dir, manifest)


def run(argv):
    parser = argparse.ArgumentParser(description=__doc__.partition("\n\n")[0])
    parser.add_argument("workspace")
    parser.add_argument("repo_url")
    parser.add_argument("commit")
    args = parser.parse_args(argv)
    main(**vars(args))


if __name__ == "__main__":
    run(sys.argv[1:])
