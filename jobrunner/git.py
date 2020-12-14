"""
Utility functions for interacting with git
"""
import logging
import os
from pathlib import Path, PurePath
import subprocess
import time
from urllib.parse import urlparse, urlunparse

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
                "ls-remote",
                "--quiet",
                "--exit-code",
                add_access_token(repo_url),
                ref,
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        output = response.stdout
    except subprocess.SubprocessError as exc:
        redact_token_from_exception(exc)
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
                    "fetch",
                    "--depth",
                    "1",
                    "--force",
                    add_access_token(repo_url),
                    commit_sha,
                ],
                check=True,
                capture_output=True,
                cwd=repo_dir,
            )
            break
        except subprocess.SubprocessError as e:
            redact_token_from_exception(e)
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


def add_access_token(repo_url):
    # We previously did a complicated thing involving the GIT_ASKPASS
    # executable which worked OK on Linux but not on Windows or macOS, so we're
    # doing the more reliable thing of just sticking the token in the URL
    token = config.PRIVATE_REPO_ACCESS_TOKEN
    if not token:
        return repo_url
    # Ensure we only ever send our token to github.com over https
    parsed = urlparse(repo_url)
    if parsed.hostname != "github.com" or parsed.scheme != "https":
        return repo_url
    # Don't overwrite existing auth details (not sure why they'd be there but
    # seems polite)
    if parsed.username or parsed.password:
        return repo_url
    # Add the token to the URL
    return urlunparse(parsed._replace(netloc=f"{token}@{parsed.netloc}"))


def redact_token_from_exception(exception):
    # The disadvantage of the above approach is it we have to do some work to
    # avoid leaking the token into the logs. However, even if it does leak it's
    # not the end of the world: the developers who have access to the logs have
    # access to the token in any case; and all it provides is read-only access
    # to some private git repos which will eventually become public in any
    # case.
    token = config.PRIVATE_REPO_ACCESS_TOKEN
    if not token:
        return
    if isinstance(exception.cmd, list):
        exception.cmd = [redact(arg, token) for arg in exception.cmd]
    else:
        exception.cmd = redact(exception.cmd, token)
    if exception.output is not None:
        exception.output = redact(exception.output, token)
    if exception.stderr is not None:
        exception.stderr = redact(exception.stderr, token)


def redact(value, secret):
    mask = "********"
    if isinstance(value, str):
        return value.replace(secret, mask)
    elif isinstance(value, bytes):
        return value.replace(secret.encode("ascii"), mask.encode("ascii"))
    # subprocess arguments can also be pathlib.Path instances (PurePath is the
    # base class for all platform-specific Path classes). We never put the
    # token in a path so there's nothing to do here
    elif isinstance(value, PurePath):
        return value
    else:
        raise ValueError(f"Got {type(value)} expected str, bytes or Path")
