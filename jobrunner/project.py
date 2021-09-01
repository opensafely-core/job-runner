import dataclasses
import json
import posixpath
import shlex
from pathlib import PurePosixPath, PureWindowsPath
from types import SimpleNamespace

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError, YAMLFutureWarning, YAMLStreamError, YAMLWarning

from jobrunner import config
from jobrunner.lib import git
from jobrunner.lib.github_validators import (
    GithubValidationError,
    validate_branch_and_commit,
    validate_repo_url,
)

# The magic action name which means "run every action"
RUN_ALL_COMMAND = "run_all"

# The version of `project.yaml` where each feature was introduced
FEATURE_FLAGS_BY_VERSION = {"UNIQUE_OUTPUT_PATH": 2, "EXPECTATIONS_POPULATION": 3}

# Build a config dict in the same format the old code expects
RUN_COMMANDS_CONFIG = {
    image: {"docker_invocation": [f"{config.DOCKER_REGISTRY}/{image}"]}
    for image in config.ALLOWED_IMAGES
}


class ProjectValidationError(Exception):
    pass


class ProjectYAMLError(ProjectValidationError):
    pass


class UnknownActionError(ProjectValidationError):
    """
    Exception which carries with it the list of valid action names for display
    to the user
    """

    def __init__(self, message, project):
        super().__init__(message)
        self.valid_actions = [RUN_ALL_COMMAND] + get_all_actions(project)


class InvalidPatternError(ProjectValidationError):
    pass


class ReusableActionError(Exception):
    """Represents a study developer-friendly reusable action error.

    We raise this in preference to other, lower-level, errors because there's only so
    much a study developer can do when there's an error with a reusable action.
    """


# Tiny dataclass to capture the specification of a project action
@dataclasses.dataclass
class ActionSpecifiction:
    run: str
    needs: list
    outputs: dict
    repo_url: str
    commit: str


@dataclasses.dataclass
class ReusableAction:
    repo_url: str
    commit: str
    action_file: bytes


def parse_yaml_file(yaml_file):
    try:
        # We're using the pure-Python version here as we don't care about speed
        # and this gives better error messages (and consistent behaviour
        # cross-platform)
        return YAML(typ="safe", pure=True).load(yaml_file)
        # ruamel doesn't have a nice exception hierarchy so we have to catch
        # these four separate base classes
    except (YAMLError, YAMLStreamError, YAMLWarning, YAMLFutureWarning) as e:
        e = make_yaml_error_more_helpful(e)
        raise ProjectYAMLError(f"{type(e).__name__} {e}")


def parse_and_validate_project_file(project_file):
    """Parse and validate the project file, handling reusable actions.

    Args:
        project_file: The contents of the project file as an immutable array of bytes.

    Returns:
        A dict representing the project.

    Raises:
        ProjectYAMLError: The contents of the project file could not be parsed.
    """
    project = parse_yaml_file(project_file)
    actions = project["actions"]
    for action_id, action in actions.items():
        actions[action_id] = handle_reusable_action(action_id, action)
    project = validate_project_and_set_defaults(project)
    return project


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
        action_config = parse_yaml_file(reusable_action.action_file)
        assert "run" in action_config
        action_run_args = shlex.split(action_config["run"])
        action_image, action_tag = action_run_args[0].split(":")
        if action_image not in config.ALLOWED_IMAGES:
            raise ProjectValidationError(f"Unrecognised runtime: {action_image}")
        if is_generate_cohort_command(action_run_args):
            raise ProjectValidationError(
                "Re-usable actions cannot invoke cohortextractor"
            )
    except (ProjectYAMLError, AssertionError, ProjectValidationError):
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


def make_yaml_error_more_helpful(exc):
    """
    ruamel produces quite helpful error messages but they refer to the file as
    `<byte_string>` (which will be confusing for users) and they also include
    notes and warnings to developers about API changes. This function attempts
    to fix these issues, but just returns the exception unchanged if anything
    goes wrong.
    """
    try:
        try:
            exc.context_mark.name = "project.yaml"
        except AttributeError:
            pass
        try:
            exc.problem_mark.name = "project.yaml"
        except AttributeError:
            pass
        exc.note = ""
        exc.warn = ""
    except Exception:
        pass
    return exc


# Copied almost verbatim from the original job-runner
def validate_project_and_set_defaults(project):
    """Check that a dictionary of project actions is valid, and set any defaults"""
    feat = get_feature_flags_for_version(project.get("version"))
    seen_runs = []
    seen_output_files = []
    if feat.EXPECTATIONS_POPULATION:
        if "expectations" not in project:
            raise ProjectValidationError("Project must include `expectations` section")
        if "population_size" not in project["expectations"]:
            raise ProjectValidationError(
                "Project `expectations` section must include `population` section",
            )
        try:
            int(project["expectations"]["population_size"])
        except TypeError:
            raise ProjectValidationError(
                "Project expectations population size must be a number",
            )
    else:
        project["expectations"] = {}
        project["expectations"]["population_size"] = 1000

    project_actions = project["actions"]

    for action_id, action_config in project_actions.items():
        if is_generate_cohort_command(shlex.split(action_config["run"])):
            if len(action_config["outputs"]) != 1:
                raise ProjectValidationError(
                    f"A `generate_cohort` action must have exactly one output; {action_id} had {len(action_config['outputs'])}",
                )

        # Check a `generate_cohort` command only generates a single output
        # Check outputs are permitted
        for privacy_level, output in action_config["outputs"].items():
            permitted_privacy_levels = [
                "highly_sensitive",
                "moderately_sensitive",
                "minimally_sensitive",
            ]
            if privacy_level not in permitted_privacy_levels:
                raise ProjectValidationError(
                    f"{privacy_level} is not valid (must be one of {', '.join(permitted_privacy_levels)})",
                )

            for output_id, filename in output.items():
                try:
                    assert_valid_glob_pattern(filename)
                except InvalidPatternError as e:
                    raise ProjectValidationError(
                        f"Output path {filename} is not permitted: {e}"
                    )

                if feat.UNIQUE_OUTPUT_PATH and filename in seen_output_files:
                    raise ProjectValidationError(
                        f"Output path {filename} is not unique"
                    )
                seen_output_files.append(filename)
        # Check it's a permitted run command

        command, *args = shlex.split(action_config["run"])
        name, _, version = command.partition(":")
        if name not in RUN_COMMANDS_CONFIG:
            raise ProjectValidationError(f"{name} is not a supported command")
        if not version:
            raise ProjectValidationError(
                f"{name} must have a version specified (e.g. {name}:0.5.2)",
            )
        # Check the run command + args signature appears only once in
        # a project
        run_signature = f"{name}_{args}"
        if run_signature in seen_runs:
            raise ProjectValidationError(
                f"{name} {' '.join(args)} appears more than once"
            )
        seen_runs.append(run_signature)

        for dependency in action_config.get("needs", []):
            if dependency not in project_actions:
                if " " in dependency:
                    raise ProjectValidationError(
                        f"`needs` actions in '{action_id}' should be separated"
                        f" with commas:\n{', '.join(dependency.split())}"
                    )
                raise ProjectValidationError(
                    f"Action '{action_id}' lists unknown action '{dependency}'"
                    f" in its `needs` config"
                )

    return project


def get_action_specification(project, action_id):
    """Get a specification for the action from the project.

    Args:
        project: A dict representing the project.
        action_id: The string ID of the action.

    Returns:
        An instance of ActionSpecification.

    Raises:
        UnknownActionError: The action was not found in the project.
        ProjectValidationError: The project was not valid.
    """
    try:
        action_spec = project["actions"][action_id]
    except KeyError:
        raise UnknownActionError(
            f"Action '{action_id}' not found in project.yaml", project
        )
    run_command = action_spec["run"]
    if "config" in action_spec:
        run_command = add_config_to_run_command(run_command, action_spec["config"])
    run_args = shlex.split(run_command)

    # Special case handling for the `cohortextractor generate_cohort` command
    if is_generate_cohort_command(run_args, require_version=1):
        # Set the size of the dummy data population, if that's what we're
        # generating.  Possibly this should be moved to the study definition
        # anyway, which would make this unnecessary.
        if config.USING_DUMMY_DATA_BACKEND:
            if "dummy_data_file" in action_spec:
                run_command += f" --dummy-data-file={action_spec['dummy_data_file']}"
            else:
                size = int(project["expectations"]["population_size"])
                run_command += f" --expectations-population={size}"
        # Automatically configure the cohortextractor to produce output in the
        # directory the `outputs` spec is expecting. Longer term I'd like to
        # just make it an error if the directories don't match, rather than
        # silently fixing it. (We can use the project versioning system to
        # ensure this doesn't break existing studies.)
        output_dirs = get_output_dirs(action_spec["outputs"])
        if len(output_dirs) != 1:
            # If we detect multiple output directories but the command
            # explicitly specifies an output directory then we assume the user
            # knows what they're doing and don't attempt to modify the output
            # directory or throw an error
            if not args_include(run_args, "--output-dir"):
                raise ProjectValidationError(
                    f"generate_cohort command should produce output in only one "
                    f"directory, found {len(output_dirs)}:\n"
                    + "\n".join([f" - {d}/" for d in output_dirs])
                )
        else:
            run_command += f" --output-dir={output_dirs[0]}"

    elif is_generate_cohort_command(run_args, require_version=2):
        # cohortextractor Version 2 expects all command line arguments to be
        # specified in the run command
        if config.USING_DUMMY_DATA_BACKEND and "--dummy-data-file" not in run_command:
            raise ProjectValidationError(
                "--dummy-data-file is required for a local run"
            )

        # There is one and only one output file in the outputs spec (verified
        # in validate_project_and_set_defaults())
        output_file = next(
            output_file
            for output in action_spec["outputs"].values()
            for output_file in output.values()
        )
        if output_file not in run_command:
            raise ProjectValidationError(
                "--output in run command and outputs must match"
            )

    elif is_generate_cohort_command(run_args):
        raise RuntimeError("Unhandled cohortextractor version")

    return ActionSpecifiction(
        run=run_command,
        needs=action_spec.get("needs", []),
        outputs=action_spec["outputs"],
        # If action_spec (a dict) represents a reusable action, then it will have the
        # following keys. If not, then it won't.
        repo_url=action_spec.get("repo_url"),
        commit=action_spec.get("commit"),
    )


def add_config_to_run_command(run_command, config):
    """Add --config flag to command.

    For commands that require complex config, users can supply a config key in
    project.yaml.  We serialize this as JSON, and pass it to the command with the
    --config flag.
    """
    config_as_json = json.dumps(config).replace("'", r"\u0027")
    return f"{run_command} --config '{config_as_json}'"


def is_generate_cohort_command(args, require_version=None):
    """
    The `cohortextractor generate_cohort` command gets special treatment in
    various places (e.g. it's the only command which gets access to the
    database) so it's helpful to have a single function for identifying it
    """
    assert not isinstance(args, str)
    version_found = None
    if len(args) > 1:
        if args[0].startswith("cohortextractor:") and args[1] == "generate_cohort":
            version_found = 1
        if args[0].startswith("cohortextractor-v2:"):
            version_found = 2
    # If we're not looking for a specific version then return True if any
    # version found
    if require_version is None:
        return version_found is not None
    # Otherwise return True only if specified version found
    else:
        return version_found == require_version


def args_include(args, target_arg):
    return any(arg == target_arg or arg.startswith(f"{target_arg}=") for arg in args)


def get_all_actions(project):
    # We ignore any manually defined run_all action (in later project versions
    # this will be an error). We use a list comprehension rather than set
    # operators as previously so we preserve the original order.
    return [action for action in project["actions"].keys() if action != RUN_ALL_COMMAND]


def get_all_output_patterns_from_project_file(project_file):
    project = parse_and_validate_project_file(project_file)
    all_patterns = set()
    for action in project["actions"].values():
        for patterns in action["outputs"].values():
            all_patterns.update(patterns.values())
    return list(all_patterns)


def get_output_dirs(output_spec):
    """
    Given the set of output files specified by an action, return a list of the
    unique directory names of those outputs
    """
    filenames = []
    for group in output_spec.values():
        filenames.extend(group.values())
    dirs = set(PurePosixPath(filename).parent for filename in filenames)
    return list(dirs)


def get_feature_flags_for_version(version):
    latest_version = max(FEATURE_FLAGS_BY_VERSION.values())
    if version is None:
        raise ProjectValidationError(
            f"Project file must have a `version` attribute specifying which "
            f"version of the project configuration format it uses (current "
            f"latest version is {latest_version})"
        )
    try:
        version = float(version)
    except (TypeError, ValueError):
        raise ProjectValidationError(
            f"`version` must be a number between 1 and {latest_version}"
        )
    feat = SimpleNamespace()
    matched_any = False
    for k, v in FEATURE_FLAGS_BY_VERSION.items():
        if v <= version:
            setattr(feat, k, True)
            matched_any = True
        else:
            setattr(feat, k, False)
    if version > 1 and not matched_any:
        raise ProjectValidationError(
            f"`version` must be a number between 1 and {latest_version}"
        )
    return feat


def assert_valid_glob_pattern(pattern):
    """
    These patterns get converted into regular expressions and matched
    with a `find` command so there shouldn't be any possibility of a path
    traversal attack anyway. But it's still good to ensure that they are
    well-formed.
    """
    # Only POSIX slashes please
    if "\\" in pattern:
        raise InvalidPatternError("contains back slashes (use forward slashes only)")
    # These aren't unsafe, but they won't behave as expected so we shouldn't let
    # people use them
    for expr in ("**", "?", "["):
        if expr in pattern:
            raise InvalidPatternError(
                f"contains '{expr}' (only the * wildcard character is supported)"
            )
    if pattern.endswith("/"):
        raise InvalidPatternError(
            "looks like a directory (only files should be specified)"
        )
    # Check that the path is in normal form
    if posixpath.normpath(pattern) != pattern:
        raise InvalidPatternError(
            "is not in standard form (contains double slashes or '..' elements)"
        )
    # This is the directory we use for storing metadata about action runs and
    # we don't want outputs getting mixed up in it.
    if pattern == "metadata" or pattern.startswith("metadata/"):
        raise InvalidPatternError("should not include the metadata directory")
    # Windows has a different notion of absolute paths (e.g c:/foo) so we check
    # for both platforms
    if PurePosixPath(pattern).is_absolute() or PureWindowsPath(pattern).is_absolute():
        raise InvalidPatternError("is an absolute path")
