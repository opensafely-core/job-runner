"""
Utility functions for interacting with git
"""
import logging
import os
import subprocess
import time
from pathlib import Path, PurePath
from urllib.parse import urlparse, urlunparse

from jobrunner import config
from jobrunner.lib.string_utils import project_name_from_url
from jobrunner.lib.subprocess_utils import subprocess_run


log = logging.getLogger(__name__)


# See `commit_already_fetched`
SENTINEL_TAG_PREFIX = "fetched/"

# Prevent git from ever prompting for credentials. Hat tip:
# https://serverfault.com/a/1054253
NEVER_PROMPT_FOR_AUTH_ENV = dict(
    os.environ,
    GIT_TERMINAL_PROMPT="0",
    GIT_ASKPASS="echo",
    GCM_INTERACTIVE="never",
)


class GitError(Exception):
    pass


class GitFileNotFoundError(GitError):
    pass


class GitRepoNotReachableError(GitError):
    pass


class GitUnknownRefError(GitError):
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


def commit_reachable_from_ref(repo_url, commit_sha, ref):
    """
    Given a `ref` (branch name, tag, etc) on a remote repo, check whether the
    supplied commit is reachable from that ref.
    """
    ref_sha = get_sha_from_remote_ref(repo_url, ref)
    # The easy case and the case I expect to be hit almost every time as the UI
    # currently only supports running against the branch head
    if commit_sha == ref_sha:
        return True
    # However a well (or badly) timed push could cause the target sha and the
    # branch head to diverge, so we need to handle that. In order to do so we
    # need to fetch the history of the branch. We first fetch just the last 10
    # commits on the assumption that it's probably one of those. If that fails
    # we fetch the entire branch history.
    repo_dir = get_local_repo_dir(repo_url)
    ensure_git_init(repo_dir)
    fetch_commit(repo_dir, repo_url, ref_sha, depth=10)
    if commit_is_ancestor(repo_dir, commit_sha, ref_sha):
        return True
    # The below is a git magic number meaning "infinite depth". See:
    # https://git-scm.com/docs/shallow
    fetch_commit(repo_dir, repo_url, ref_sha, depth=2147483647)
    return commit_is_ancestor(repo_dir, commit_sha, ref_sha)


def get_sha_from_remote_ref(repo_url, ref):
    """Gets the SHA of the commit associated with the ref at the repo URL.

    Args:
        repo_url: A repo URL.
        ref: A ref, such as a branch name, tag name, etc.

    Returns:
        The SHA of the commit. For example, if the ref is an annotated tag, then the SHA
        will be that of the associated commit, rather than that of the annotated tag.

    Raises:
        GitRepoNotReachableError: We couldn't read from the remote repo
        GitUnknownRefError: We couldn't find the specified ref in the remote repo
    """
    # If `ref` matches an annotated tag, then `deref_ref` will match the associated
    # commit.
    deref_ref = f"{ref}^{{}}"
    try:
        response = subprocess_run(
            [
                "git",
                "ls-remote",
                "--quiet",
                add_access_token_and_proxy(repo_url),
                ref,
                deref_ref,
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=NEVER_PROMPT_FOR_AUTH_ENV,
        )
        output = response.stdout
    except subprocess.SubprocessError as exc:
        redact_token_from_exception(exc)
        log.exception("Error reading from remote repository")
        raise GitRepoNotReachableError(f"Could not read from {repo_url}")
    results = _parse_ls_remote_output(output)
    for target_ref in [
        ref,  # e.g. HEAD
        f"refs/heads/{ref}",  # Branch
        f"refs/tags/{deref_ref}",  # Annotated tag
        f"refs/tags/{ref}",  # Lightweight tag
    ]:
        if target_ref in results:
            return results[target_ref]
    raise GitUnknownRefError(f"Could not find ref '{ref}' in {repo_url}")


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
    ensure_git_init(repo_dir)
    # It's safe to keep re-fetching the same commit, but it requires
    # talking to the remote repo every time so it's better to avoid it if
    # we can
    if not commit_already_fetched(repo_dir, commit_sha):
        fetch_commit(repo_dir, repo_url, commit_sha)


def ensure_git_init(repo_dir):
    if not os.path.exists(repo_dir / "config"):
        subprocess_run(["git", "init", "--bare", "--quiet", repo_dir], check=True)


def commit_already_fetched(repo_dir, commit_sha):
    """
    Return whether a given commit exists in a repo directory

    We used to do this with:

        git cat-file -e 'COMMIT_SHA^{commit}'

    However it's possible that an interrupted fetch leaves the commit object in
    place without all of its associated blobs, meaning that the above check
    passes but attempting to check out the commit will fail. To work around
    this we create a special "sentinel" tag for each commit to indicate that
    the entire fetch process has completed successfully.
    """
    response = subprocess_run(
        [
            "git",
            "tag",
            "--list",
            SENTINEL_TAG_PREFIX + commit_sha,
            "--points-at",
            commit_sha,
            "--format",
            "exists",
        ],
        check=True,
        capture_output=True,
        cwd=repo_dir,
    )
    return response.stdout.strip() == b"exists"


def mark_commmit_as_fetched(repo_dir, commit_sha):
    """
    Create a special "sentinel" tag to indicate that the supplied commit has
    been fully fetched (see `commit_already_fetched` above)
    """
    subprocess_run(
        [
            "git",
            "tag",
            "--force",
            SENTINEL_TAG_PREFIX + commit_sha,
            commit_sha,
        ],
        check=True,
        capture_output=True,
        cwd=repo_dir,
    )


def fetch_commit(repo_dir, repo_url, commit_sha, depth=1):
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
    authenticated_url = add_access_token_and_proxy(repo_url)
    while True:
        try:
            subprocess_run(
                [
                    "git",
                    "fetch",
                    "--force",
                    "--depth",
                    str(depth),
                    authenticated_url,
                    commit_sha,
                ],
                check=True,
                capture_output=True,
                cwd=repo_dir,
                env=NEVER_PROMPT_FOR_AUTH_ENV,
            )
            mark_commmit_as_fetched(repo_dir, commit_sha)
            break
        except subprocess.SubprocessError as e:
            redact_token_from_exception(e)
            log.exception(f"Error fetching commit (attempt {attempt}/{max_retries})")
            if (
                b"GnuTLS recv error" in e.stderr
                or b"SSL_read: Connection was reset" in e.stderr
            ):
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


def commit_is_ancestor(repo_dir, ancestor_sha, descendant_sha):
    response = subprocess_run(
        ["git", "merge-base", "--is-ancestor", ancestor_sha, descendant_sha],
        cwd=repo_dir,
        capture_output=True,
    )
    return response.returncode == 0


def add_access_token_and_proxy(repo_url):
    # We've already validated that the repo url starts with https://github.com
    repo_url = repo_url.replace("github.com", config.GIT_PROXY_DOMAIN)
    # We previously did a complicated thing involving the GIT_ASKPASS
    # executable which worked OK on Linux but not on Windows or macOS, so we're
    # doing the more reliable thing of just sticking the token in the URL
    token = config.PRIVATE_REPO_ACCESS_TOKEN
    if not token:
        return repo_url
    # Ensure we only ever send our token to github.com over https
    parsed = urlparse(repo_url)
    if parsed.hostname != config.GIT_PROXY_DOMAIN or parsed.scheme != "https":
        return repo_url
    # Don't overwrite existing auth details (not sure why they'd be there but
    # seems polite)
    if parsed.username or parsed.password:
        return repo_url
    # Github accepts arbitrary usernames when using a PAT so this is just for
    # easy identification in the proxy logs
    username = f"jobrunner-{config.BACKEND}"
    # Add the token to the URL
    return urlunparse(parsed._replace(netloc=f"{username}:{token}@{parsed.netloc}"))


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
