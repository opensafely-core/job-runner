import dataclasses
import shlex

from jobrunner import config
from jobrunner.lib import git
from jobrunner.lib.github_validators import (
    GithubValidationError,
    validate_branch_and_commit,
    validate_repo_url,
)
from jobrunner.lib.yaml_utils import YAMLError, parse_yaml
from jobrunner.project import ProjectValidationError, is_generate_cohort_command


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
            action_dict = handle_reusable_action({"run": job.run_command})
        except ReusableActionError as e:
            # Annotate the exception with the context of the action in which it
            # occured
            context = f"{job.action}: {job.run_command.split()[0]}"
            raise ReusableActionError(f"in '{context}' {e}") from e
        job.run_command = action_dict["run"]
        job.action_repo_url = action_dict.get("repo_url")
        job.action_commit = action_dict.get("commit")


def handle_reusable_action(action):
    """If `action` is reusable, then handle it. If not, then return it unchanged.

    Args:
        action: The action's representation as a dict. This is the action's value in
            project.yaml.

    Returns:
        The action's representation as a dict. If `action` resolves to a reusable
        action, then it is rewritten to point to the reusable action and a copy is
        returned. If not, then `action` is returned unchanged.

    Raises:
        ReusableActionError: An error occurred when accessing the reusable action.
    """
    run_args = shlex.split(action["run"])
    image, tag = run_args[0].split(":")

    if image in config.ALLOWED_IMAGES:
        # This isn't a reusable action.
        return action

    reusable_action = fetch_reusable_action(image, tag)
    new_action = apply_reusable_action(action, reusable_action)
    return new_action


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


def apply_reusable_action(action, reusable_action):
    """
    Rewrite an `action` dict to run the code specifed by the supplied
    `ReusableAction` instance.

    Args:
        action: The action's representation as a dict. This is the action's value in
            project.yaml.
        reusable_action: A ReusableAction instance

    Returns:
        The modified action's representation as a dict.

    Raises:
        ReusableActionError: An error occurred when accessing the reusable action.
    """
    try:
        # If there's a problem, then it relates to the reusable action. The study
        # developer didn't make an error; the reusable action developer did.
        action_config = parse_yaml(reusable_action.action_file, name="action.yaml")
        assert "run" in action_config
        action_run_args = shlex.split(action_config["run"])
        action_image, action_tag = action_run_args[0].split(":")
        if action_image not in config.ALLOWED_IMAGES:
            raise ProjectValidationError(f"Unrecognised runtime: {action_image}")
        if is_generate_cohort_command(action_run_args):
            raise ProjectValidationError(
                "Re-usable actions cannot invoke cohortextractor"
            )
    except (YAMLError, AssertionError, ProjectValidationError):
        raise ReusableActionError(
            f"invalid action, please open an issue on "
            f"{reusable_action.repo_url}/issues"
        )

    # ["action:tag", "arg", ...] -> ["runtime:tag binary entrypoint", "arg", ...]
    run_args = shlex.split(action["run"])
    run_args[0] = action_config["run"]

    new_action = action.copy()
    new_action["run"] = " ".join(run_args)
    new_action["repo_url"] = reusable_action.repo_url
    new_action["commit"] = reusable_action.commit
    return new_action
