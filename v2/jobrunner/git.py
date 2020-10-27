import os
from pathlib import Path
import subprocess
from urllib.parse import urlparse

from . import config


class GitError(Exception):
    pass


def read_file_from_repo(repo_url, commit_sha, path):
    """
    Return the contents of the file at `path` in `repo_url` as of `commit_sha`
    """
    repo_dir = get_local_repo_dir(repo_url)
    fetch_commit(repo_dir, repo_url, commit_sha)
    try:
        response = subprocess.run(
            ["git", "show", f"{commit_sha}:{path}"],
            capture_output=True,
            check=True,
            cwd=repo_dir,
        )
    except subprocess.SubprocessError:
        raise GitError(f"Error reading file: {path}")
    # Note the response here is bytes not text as git doesn't know what
    # encoding the file is supposed to have
    return response.stdout


def checkout_commit(repo_url, commit_sha, target_dir):
    """
    Checkout the contents of `repo_url` as of `commit_sha` into `target_dir`
    """
    repo_dir = get_local_repo_dir(repo_url)
    fetch_commit(repo_dir, repo_url, commit_sha)
    os.makedirs(target_dir, exist_ok=True)
    subprocess.run(
        [
            "git",
            f"--work-tree={target_dir}",
            "checkout",
            "--quiet",
            "--force",
            commit_sha,
        ],
        check=True,
        # Set GIT_DIR rather than changing working directory so that
        # `target_dir` gets correctly resolved
        env=dict(os.environ, GIT_DIR=repo_dir),
    )


def get_sha_from_remote_ref(repo_url, ref):
    """
    Given a `ref` (branch name, tag, etc) on a remote repo, turn it into a
    commit SHA.

    In future we might not need this as the job-server should only supply us
    with SHAs, but for now we want to be able to accept branch names and
    transform them into SHAs.
    """
    try:
        response = subprocess.run(
            ["git", "ls-remote", "--quiet", "--exit-code", repo_url, ref],
            check=True,
            capture_output=True,
            env=supply_access_token(repo_url),
            text=True,
            encoding="utf-8",
        )
        output = response.stdout
    except subprocess.SubprocessError:
        output = ""
    results = _parse_ls_remote_output(output)
    if len(results) == 1:
        return list(results.values())[0]
    elif len(results) > 1:
        # Where we have more than one match, but one is an exact match for a
        # local branch then use that result. (This happens when using local
        # repos where there are references to both the local and remote
        # branches.)
        target_ref = f"refs/heads/{ref}"
        if target_ref in results:
            return results[target_ref]
        else:
            raise GitError(f"Ambiguous ref '{ref}' in {repo_url}")
    else:
        raise GitError(f"Error resolving ref '{ref}' from {repo_url}")


def _parse_ls_remote_output(output):
    lines = [line.split() for line in output.splitlines()]
    return {line[1]: line[0] for line in lines}


def get_local_repo_dir(repo_url):
    # We don't need to worry that this transformation might not result in
    # unique names (e.g. if we end up using repos from different
    # organisations). We're just treating these directories as big buckets of
    # commits, so we could in principle use the same local git directory for
    # everything and it would work fine.
    repo_name = urlparse(repo_url).path.strip("/").split("/")[-1]
    return config.GIT_REPO_DIR / Path(repo_name)


def fetch_commit(repo_dir, repo_url, commit_sha):
    if not os.path.exists(repo_dir / "config"):
        subprocess.run(["git", "init", "--bare", "--quiet", repo_dir], check=True)
    # It's safe to keep re-fetching the same commit, but it requires talking to
    # the remote repo every time so it's better to avoid it if we can
    elif commit_already_fetched(repo_dir, commit_sha):
        return
    try:
        subprocess.run(
            ["git", "fetch", "--depth", "1", "--force", repo_url, commit_sha],
            check=True,
            capture_output=True,
            cwd=repo_dir,
            env=supply_access_token(repo_url),
        )
    except subprocess.SubprocessError:
        raise GitError(f"Error fetching commit {commit_sha} from {repo_url}")


def commit_already_fetched(repo_dir, commit_sha):
    response = subprocess.run(
        ["git", "cat-file", "-e", f"{commit_sha}^{{commit}}"],
        capture_output=True,
        cwd=repo_dir,
    )
    return response.returncode == 0


def supply_access_token(repo_url):
    token = config.PRIVATE_REPO_ACCESS_TOKEN
    # Ensure we only ever send our token to github.com over https
    parsed = urlparse(repo_url)
    if parsed.hostname != "github.com" or parsed.scheme != "https":
        token = ""
    return dict(
        os.environ,
        # This script will supply as the username the access token from
        # the environment variable GIT_ACCESS_TOKEN
        GIT_ASKPASS=Path(__file__).parent / "git_askpass_access_token.py",
        GIT_ACCESS_TOKEN=token,
    )
