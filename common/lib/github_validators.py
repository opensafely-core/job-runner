from urllib.parse import urlparse

from common.lib.git import GitUnknownRefError, commit_reachable_from_ref


class GithubValidationError(Exception):
    pass


def validate_repo_and_commit(allowed_gitub_orgs, repo_url, commit, branch):
    if not allowed_gitub_orgs:
        # If we're not restricting to specific Github organisations then there's no
        # point in checking the provenance of the supplied commit
        return

    # just validating http prefix allows local git repos, useful for tests
    if repo_url.startswith("http"):
        validate_repo_url(repo_url, allowed_gitub_orgs)
        validate_branch_and_commit(repo_url, commit, branch)


def validate_repo_url(repo_url, allowed_gitub_orgs):
    parsed_url = urlparse(repo_url)
    if parsed_url.scheme != "https" or parsed_url.netloc != "github.com":
        raise GithubValidationError("Repository URLs must start https://github.com")
    path = parsed_url.path.strip("/").split("/")
    if not path or path[0] not in allowed_gitub_orgs:
        raise GithubValidationError(
            f"Repositories must belong to one of the following Github "
            f"organisations: {' '.join(allowed_gitub_orgs)}"
        )
    expected_url = f"https://github.com/{'/'.join(path[:2])}"
    if repo_url.rstrip("/") != expected_url or len(path) != 2:
        raise GithubValidationError(
            "Repository URL was not of the expected format: "
            "https://github.com/[organisation]/[project-name]"
        )


def validate_branch_and_commit(repo_url, commit, branch):
    """
    Due to the way Github works, anyone who can open a pull request against a
    repository can make a commit appear to be "in" that repository, even if
    they do not have write access to it.

    For example, someone created this PR against the Linux kernel:
    https://github.com/torvalds/linux/pull/437

    And even though this will never be merged, it still appears as a commit in
    that repo:
    https://github.com/torvalds/linux/commit/2793ae1df012c7c3f13ea5c0f0adb99017999c3b

    If we are enforcing that only code from certain organisations can be run
    then we need to check that any commits supplied have been made by someone
    with write access to the repository, which means we need to check they
    belong to a branch or tag in the repository.
    """
    if not branch:
        raise GithubValidationError("A branch name must be supplied")
    # A further wrinkle is that each PR gets an associated ref within the repo of the
    # form `pull/PR_NUMBER/head`. By checking against `refs/heads/{branch}` we ensure
    # that we're dealing with a genuine branch in the repo and not a pull request.
    #
    # Note that despite their path-like appearance these references are not actually
    # paths and so traversal attacks using `../../` sequences have no effect.
    try:
        is_reachable = commit_reachable_from_ref(
            repo_url, commit, f"refs/heads/{branch}"
        )
    except GitUnknownRefError:
        raise GithubValidationError(f"Could not find branch '{branch}'")
    if not is_reachable:
        raise GithubValidationError(
            f"Could not find commit on branch '{branch}': {commit}"
        )
