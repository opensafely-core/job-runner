"""
Utility functions for interacting with git
"""
import logging
import os
from pathlib import Path
import subprocess
import time
from urllib.parse import urlparse

from . import config
from .string_utils import project_name_from_url
from .subprocess_utils import subprocess_run


log = logging.getLogger(__name__)


class GitError(Exception):
    pass


class GitFileNotFoundError(GitError):
    pass


def read_file_from_repo(repo_url, commit_sha, path):
    """
    Return the contents of the file at `path` in `repo_url` as of `commit_sha`
    """
    repo_dir = get_local_repo_dir(repo_url)
    ensure_commit_fetched(repo_dir, repo_url, commit_sha)
    try:
        response = subprocess_run(
            ["git", "show", f"{commit_sha}:{path}"],
            capture_output=True,
            check=True,
            cwd=repo_dir,
        )
    except subprocess.SubprocessError as e:
        if e.stderr.startswith(b"fatal: path ") and b"does not exist" in e.stderr:
            raise GitFileNotFoundError(f"File '{path}' not found in repository")
        else:
            log.exception(f"Error reading from {repo_url} @ {commit_sha}")
            raise GitError(f"Error reading from {repo_url} @ {commit_sha}")
    # Note the response here is bytes not text as git doesn't know what
    # encoding the file is supposed to have
    return response.stdout


def checkout_commit(repo_url, commit_sha, target_dir):
    """
    Checkout the contents of `repo_url` as of `commit_sha` into `target_dir`
    """
    repo_dir = get_local_repo_dir(repo_url)
    ensure_commit_fetched(repo_dir, repo_url, commit_sha)
    os.makedirs(target_dir, exist_ok=True)
    subprocess_run(
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
        response = subprocess_run(
            [
                "git",
                *auth_arguments(),
                "ls-remote",
                "--quiet",
                "--exit-code",
                repo_url,
                ref,
            ],
            check=True,
            capture_output=True,
            env=supply_access_token(repo_url),
            text=True,
            encoding="utf-8",
        )
        output = response.stdout
    except subprocess.SubprocessError:
        log.exception(f"Error resolving {ref} from {repo_url}")
        output = ""
    results = _parse_ls_remote_output(output)
    if len(results) == 1:
        return list(results.values())[0]
    elif len(results) > 1:
        # Where we have more than one match, but there is either an exact match
        # or a match for a local branch then use that result. (This happens
        # when using local repos where there are references to both the local
        # and remote branches.)
        for target_ref in [ref, f"refs/heads/{ref}"]:
            if target_ref in results:
                return results[target_ref]
        raise GitError(f"Ambiguous ref '{ref}' in {repo_url}")
    else:
        raise GitError(f"Error resolving ref '{ref}' from {repo_url}")


def _parse_ls_remote_output(output):
    lines = [line.split() for line in output.splitlines()]
    return {line[1]: line[0] for line in lines}


def get_local_repo_dir(repo_url):
    # We don't need to worry that repo_name may not be unique here (e.g. if we
    # end up using repos from different organisations): we're just treating
    # these directories as big buckets of commits, so we could in principle use
    # the same local git directory for all repositories and it would work fine.
    # But it's probably more operationally convenient to split them up like
    # this.
    repo_name = project_name_from_url(repo_url)
    return config.GIT_REPO_DIR / Path(repo_name).with_suffix(".git")


def ensure_commit_fetched(repo_dir, repo_url, commit_sha):
    if not os.path.exists(repo_dir / "config"):
        subprocess_run(["git", "init", "--bare", "--quiet", repo_dir], check=True)
        fetched = False
    # It's safe to keep re-fetching the same commit, but it requires talking to
    # the remote repo every time so it's better to avoid it if we can
    else:
        fetched = commit_already_fetched(repo_dir, commit_sha)
    if not fetched:
        fetch_commit(repo_dir, repo_url, commit_sha)


def commit_already_fetched(repo_dir, commit_sha):
    response = subprocess_run(
        ["git", "cat-file", "-e", f"{commit_sha}^{{commit}}"],
        capture_output=True,
        cwd=repo_dir,
    )
    return response.returncode == 0


def fetch_commit(repo_dir, repo_url, commit_sha):
    # The unfortunate retry complexity here is due to mysterious errors we
    # sometimes get when fetching commits in the live environment:
    #
    #   error: RPC failed; curl 56 GnuTLS recv error (-9): A TLS packed with unexpected length was received
    #
    # They are mostly transient (hence the retries) but certain commits seem to
    # trigger them more often than others so presumably it's something to do
    # with the precise sequence of packets that get sent. More details here:
    # https://github.com/opensafely/job-runner/issues/5
    max_retries = 5
    sleep = 4
    attempt = 1
    while True:
        try:
            subprocess_run(
                [
                    "git",
                    *auth_arguments(),
                    "fetch",
                    "--depth",
                    "1",
                    "--force",
                    repo_url,
                    commit_sha,
                ],
                check=True,
                capture_output=True,
                cwd=repo_dir,
                env=supply_access_token(repo_url),
            )
            break
        except subprocess.SubprocessError as e:
            log.exception(
                f"Error fetching commit {commit_sha} from {repo_url}"
                f" (attempt {attempt}/{max_retries})"
            )
            if b"GnuTLS recv error" in e.stderr:
                attempt += 1
                if attempt > max_retries:
                    raise GitError(
                        f"Network error when fetching commit {commit_sha} from"
                        f" {repo_url}\n"
                        "(This may work if you try again later)"
                    )
                else:
                    time.sleep(sleep)
                    sleep *= 2
            else:
                raise GitError(f"Error fetching commit {commit_sha} from {repo_url}")


def auth_arguments():
    """
    Adds authentication related arguments to git invocations
    """
    # This script will supply as the username the access token from the
    # environment variable GIT_ACCESS_TOKEN
    askpath_exec = Path(__file__).parent / "git_askpass_access_token.py"
    return [
        # Disable the default credentials helper so git never tries to pop up a
        # modal dialog or anyting awful like that
        "-c",
        "credential.helper=''",
        # Use our askpath executable
        "-c",
        f"core.askpass={askpath_exec}",
    ]


def supply_access_token(repo_url):
    token = config.PRIVATE_REPO_ACCESS_TOKEN
    # Ensure we only ever send our token to github.com over https
    parsed = urlparse(repo_url)
    if parsed.hostname != "github.com" or parsed.scheme != "https":
        token = ""
    return dict(os.environ, GIT_ACCESS_TOKEN=token)
