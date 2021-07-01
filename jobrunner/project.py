import dataclasses
import json
from pathlib import PureWindowsPath, PurePosixPath
import posixpath
import shlex
from types import SimpleNamespace

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError, YAMLStreamError, YAMLWarning, YAMLFutureWarning

from . import config


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


# Tiny dataclass to capture the specification of a project action
@dataclasses.dataclass
class ActionSpecifiction:
    run: str
    needs: list
    outputs: dict


def parse_and_validate_project_file(project_file):
    try:
        # We're using the pure-Python version here as we don't care about speed
        # and this gives better error messages (and consistent behaviour
        # cross-platform)
        project = YAML(typ="safe", pure=True).load(project_file)
        # ruamel doesn't have a nice exception hierarchy so we have to catch
        # these four separate base classes
    except (YAMLError, YAMLStreamError, YAMLWarning, YAMLFutureWarning) as e:
        e = make_yaml_error_more_helpful(e)
        raise ProjectYAMLError(f"{type(e).__name__} {e}")
    project = validate_project_and_set_defaults(project)
    return project


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
    """
    Given a project and action, return an ActionSpecification which contains
    everything the job-runner needs to run this action
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
    # Specical case handling for the `cohortextractor generate_cohort` command
    if is_generate_cohort_command(run_args):
        # Set the size of the dummy data population, if that's what were
        # generating.  Possibly this should be moved to the study definition
        # anyway, which would make this unnecessary.
        if config.USING_DUMMY_DATA_BACKEND:
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
    return ActionSpecifiction(
        run=run_command,
        needs=action_spec.get("needs", []),
        outputs=action_spec["outputs"],
    )


def add_config_to_run_command(run_command, config):
    """Add --config flag to command.

    For commands that require complex config, users can supply a config key in
    project.yaml.  We serialize this as JSON, and pass it to the command with the
    --config flag.
    """
    config_as_json = json.dumps(config).replace("'", r"\u0027")
    return f"{run_command} --config '{config_as_json}'"


def is_generate_cohort_command(args):
    """
    The `cohortextractor generate_cohort` command gets special treatment in
    various places (e.g. it's the only command which gets access to the
    database) so it's helpful to have a single function for identifying it
    """
    assert not isinstance(args, str)
    if (
        len(args) > 1
        and args[0].startswith("cohortextractor:")
        and args[1] == "generate_cohort"
    ):
        return True
    return False


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


def assert_valid_actions(project, actions):
    if not actions:
        raise UnknownActionError("At least one action must be supplied", project)
    for action in actions:
        if action != RUN_ALL_COMMAND and action not in project["actions"]:
            raise UnknownActionError(
                f"Action '{action}' not found in project.yaml", project
            )
