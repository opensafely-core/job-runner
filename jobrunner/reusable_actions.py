import dataclasses
import shlex
import textwrap

from jobrunner import config
from jobrunner.lib import git
from jobrunner.lib.github_validators import (
    GithubValidationError,
    validate_branch_and_commit,
    validate_repo_url,
)
from jobrunner.lib.yaml_utils import YAMLError, parse_yaml
from jobrunner.project import is_generate_cohort_command


class ReusableActionError(Exception):
    """Represents a study developer-friendly reusable action error.

    We raise this in preference to other, lower-level, errors because there's only so
    much a study developer can do when there's an error with a reusable action.
    """


@dataclasses.dataclass
class ReusableAction:
    repo_url: str
    commit: str
    action_file: bytes


def resolve_reusable_action_references(jobs):
    """
    Accepts a list of Job instances, identifies any which invoke reusable
    actions and modifies them appropriately which means:
        * rewriting their `run` command to use the entrypoint defined by the
          reusable action
        * adding a reference to the reusable action's repo and commit

    Args:
        jobs: list of Job instances

    Returns:
        None - it modifies its arguments in place

    Raises:
        ReusableActionError
    """
    for job in jobs:
        try:
            run_command, repo_url, commit = handle_reusable_action(job.run_command)
        except ReusableActionError as e:
            # Annotate the exception with the context of the action in which it
            # occured
            context = f"{job.action}: {job.run_command.split()[0]}"
            raise ReusableActionError(f"in '{context}' {e}") from e
        job.run_command = run_command
        job.action_repo_url = repo_url
        job.action_commit = commit


def handle_reusable_action(run_command):
    """
    If `run_command` refers to a reusable action then rewrite it appropriately
    and return it along with the repo_url and commit of the reusable action.
    Otherwise return it unchanged with null values for repo_url and commit.

    Args:
        run_command: Action's run command as a string

    Returns: tuple consisting of
        - rewritten_run_command: string
        - resuable_action_repo_url: string or None if not a reusable action
        - reusable_action_commit: string or None if not a reusable action

    Raises:
        ReusableActionError: Something was wrong with the reusable action
    """
    run_args = shlex.split(run_command)
    image, tag = run_args[0].split(":")

    if image in config.ALLOWED_IMAGES:
        # This isn't a reusable action, nothing to do
        return run_command, None, None

    reusable_action = fetch_reusable_action(image, tag)
    new_run_args = apply_reusable_action(run_args, reusable_action)
    new_run_command = shlex.join(new_run_args)
    return new_run_command, reusable_action.repo_url, reusable_action.commit


def fetch_reusable_action(image, tag):
    """
    Fetch all metadata from git needed to apply a reusable action

    Args:
        image: The name of the reusable action
        tag: The specified version of the reusable action

    Returns:
        ReusableAction object, wrapping the repo_url, commit and the contents
        of the `action.yaml` file

    Raises:
        ReusableActionError: An error occurred when accessing the reusable action.
    """
    repo_url = f"{config.ACTIONS_GITHUB_ORG_URL}/{image}"
    try:
        validate_repo_url(repo_url, [config.ACTIONS_GITHUB_ORG])
    except GithubValidationError:
        raise ReusableActionError(f"'{image}' contains invalid characters")

    try:
        # If there's a problem, then it relates to the repository. Maybe the study
        # developer made an error; maybe the reusable action developer made an error.
        commit = git.get_sha_from_remote_ref(repo_url, tag)
    except git.GitRepoNotReachableError:
        raise ReusableActionError(
            f"could not find a repo at {repo_url}\n"
            f"Check that '{image}' is in the list of available actions at "
            f"https://actions.opensafely.org"
        )
    except git.GitUnknownRefError:
        raise ReusableActionError(f"'{tag}' is not a tag listed in {repo_url}/tags")

    # We're planning to give write access to specific external collaborators on
    # specific action repos, but we want to retain final control over what gets
    # deployed. Github's permissions model doesn't (yet) allow us to restrict
    # what tags they can push, but we can restrict access to certain branches.
    # So here we check that the tag refers to a commit which has been merged to
    # main and refuse to run if not. If Github ever supports restricting tag
    # creation then we can do away with this check.
    try:
        validate_branch_and_commit(repo_url, commit, "main")
    except GithubValidationError:
        raise ReusableActionError(
            f"tag '{tag}' has not yet been approved for use (not merged into main branch)"
        )
    except git.GitError:
        # Our git library already logs the relevant details here so throwing
        # away the original exception is fine
        raise ReusableActionError(f"error validating '{commit}' in {repo_url}")

    try:
        # If there's a problem, then it relates to the reusable action. The study
        # developer didn't make an error; the reusable action developer did.
        action_file = git.read_file_from_repo(repo_url, commit, "action.yaml")
    except git.GitFileNotFoundError:
        raise ReusableActionError(
            f"{repo_url}/tree/{tag} doesn't look like a valid action "
            f"(no 'action.yaml' file present)"
        )
    except git.GitError:
        # Our git library already logs the relevant details here so throwing
        # away the original exception is fine
        raise ReusableActionError(f"error reading '{commit}' from {repo_url}")

    return ReusableAction(repo_url=repo_url, commit=commit, action_file=action_file)


def apply_reusable_action(run_args, reusable_action):
    """
    Rewrite a list of "run" arguments to run the code specifed by the supplied
    `ReusableAction` instance.

    Args:
        run_args: Action's run command as a list of string arguments
        reusable_action: A ReusableAction instance

    Returns:
        The modified run arguments as a list

    Raises:
        ReusableActionError: An error occurred when accessing the reusable action.
    """
    try:
        # If there's a problem, then it relates to the reusable action. The study
        # developer didn't make an error; the reusable action developer did.
        action_config = parse_yaml(reusable_action.action_file, name="action.yaml")
        if "run" not in action_config:
            raise ReusableActionError("Missing `run` key in 'action.yaml'")
        action_run_args = shlex.split(action_config["run"])
        action_image, action_tag = action_run_args[0].split(":")
        if action_image not in config.ALLOWED_IMAGES:
            raise ReusableActionError(f"Unrecognised runtime: {action_image}")
        if is_generate_cohort_command(action_run_args):
            raise ReusableActionError(
                "Re-usable actions cannot invoke cohortextractor/databuilder"
            )
    except (YAMLError, ReusableActionError) as e:
        formatted_error = textwrap.indent(f"{type(e).__name__}: {e}", "  ")
        raise ReusableActionError(
            f"invalid action, please open an issue on "
            f"{reusable_action.repo_url}/issues\n\n"
            f"{formatted_error}"
        )

    # ["action:tag", "arg", ...] -> ["runtime:tag binary entrypoint", "arg", ...]
    return action_run_args + run_args[1:]
