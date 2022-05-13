from __future__ import print_function, unicode_literals, division, absolute_import

from argparse import ArgumentParser
from pathlib import Path

from jobrunner.executors.graphnet.container.utils import copy_files
from jobrunner.lib import git
from jobrunner.lib.string_utils import project_name_from_url


def main():
    """
    Preprocessing before running the opensafely job action
    
    1. checkout code of the study if not done yet
    2. move the file to the right place for job action to run in the next step
    """
    
    parser = ArgumentParser(
            description="Preprocess before running the opensafely job action"
    )
    
    # Cohort parser options
    parser.add_argument("repo_url", type=str, help="The URL of the git repository of the study")
    parser.add_argument("commit_sha", type=str, help="The sha of the commit using in the repository")
    parser.add_argument("repo_root", type=str, help="The dir to store the git repository. Should be the workspace volume. e.g. /ws_volume/workdir/repos")
    parser.add_argument("workspace_dir", type=str, help="The dir of the workspace. e.g. workdir/high_privacy/workspaces/workspace_name")
    parser.add_argument("job_dir", type=str, help="The dir to store the git commit and input files. Should be the job volume. e.g. /job_volume/workspace")
    parser.add_argument("inputs", type=str, help="The paths joined by ; which need to be copied from workspace volume to job volume")
    
    args = parser.parse_args()
    
    repo_url = args.repo_url
    commit_sha = args.commit_sha
    repo_root = Path(args.repo_root)
    workspace_dir = Path(args.workspace_dir)
    job_dir = Path(args.job_dir)
    inputs = args.inputs.split(";")
    
    git_clone_and_checkout(repo_url, commit_sha, repo_root, job_dir)
    
    # relative to workspace_dir
    copy_files(workspace_dir, inputs, job_dir)


def git_clone_and_checkout(repo_url, commit_sha, repo_root, job_dir):
    """
    Git clone the repo (if not exists) to the workspace repo directory and then checkout the commit to the job directory
    """
    
    # reuse the same method in the docker flow
    repo_name = project_name_from_url(repo_url)
    repo_dir = Path(repo_root) / Path(repo_name).with_suffix(".git")
    
    git.ensure_commit_fetched(repo_dir, repo_url, commit_sha)
    git.checkout_commit(repo_url, commit_sha, job_dir)


if __name__ == '__main__':
    main()
