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
        action_dict = handle_reusable_action(job.action, {"run": job.run_command})
        job.run_command = action_dict["run"]
        job.action_repo_url = action_dict.get("repo_url")
        job.action_commit = action_dict.get("commit")


def handle_reusable_action(action_id, action):
    """If `action` is reusable, then handle it. If not, then return it unchanged.

    Args:
        action_id: The action's ID as a string. This is the action's key in
            project.yaml. It is used to raise errors with more informative messages.
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

    reusable_action = fetch_reusable_action(action_id, image, tag)
    new_action = apply_reusable_action(action_id, action, reusable_action)
    return new_action


def fetch_reusable_action(action_id, image, tag):
    """
    Fetch all metadata from git needed to apply a reusable action

    Args:
        action_id: The action's ID as a string. This is the action's key in
            project.yaml. It is used to raise errors with more informative messages.
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
    except GithubValidationError as e:
        raise ReusableActionError(*e.args)  # This keeps the function signature clean

    try:
        # If there's a problem, then it relates to the repository. Maybe the study
        # developer made an error; maybe the reusable action developer made an error.
        commit = git.get_sha_from_remote_ref(repo_url, tag)
    except git.GitError:
        raise ReusableActionError(
            f"Cannot resolve '{action_id}' to a repository at '{repo_url}'"
        )

    try:
        validate_branch_and_commit(repo_url, commit, "main")
    except GithubValidationError as e:
        raise ReusableActionError(*e.args)

    try:
        # If there's a problem, then it relates to the reusable action. The study
        # developer didn't make an error; the reusable action developer did.
        action_file = git.read_file_from_repo(repo_url, commit, "action.yaml")
    except git.GitError:
        raise ReusableActionError(
            f"There is a problem with the reusable action required by '{action_id}'"
        )

    return ReusableAction(repo_url=repo_url, commit=commit, action_file=action_file)


def apply_reusable_action(action_id, action, reusable_action):
    """
    Rewrite an `action` dict to run the code specifed by the supplied
    `ReusableAction` instance.

    Args:
        action_id: The action's ID as a string. This is the action's key in
            project.yaml. It is used to raise errors with more informative messages.
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
            f"There is a problem with the reusable action required by '{action_id}'"
        )

    # ["action:tag", "arg", ...] -> ["runtime:tag binary entrypoint", "arg", ...]
    run_args = shlex.split(action["run"])
    run_args[0] = action_config["run"]

    new_action = action.copy()
    new_action["run"] = " ".join(run_args)
    new_action["repo_url"] = reusable_action.repo_url
    new_action["commit"] = reusable_action.commit
    return new_action
