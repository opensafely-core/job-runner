import dataclasses
import json
import posixpath
import shlex
from pathlib import PurePosixPath, PureWindowsPath
from types import SimpleNamespace

from jobrunner import config
from jobrunner.lib.yaml_utils import YAMLError, parse_yaml


# The magic action name which means "run every action"
RUN_ALL_COMMAND = "run_all"

# The version of `project.yaml` where each feature was introduced
FEATURE_FLAGS_BY_VERSION = {"UNIQUE_OUTPUT_PATH": 2, "EXPECTATIONS_POPULATION": 3}


class ProjectValidationError(Exception):
    pass


class UnknownActionError(ProjectValidationError):
    pass


class InvalidPatternError(ProjectValidationError):
    pass


# Tiny dataclass to capture the specification of a project action
@dataclasses.dataclass
class ActionSpecifiction:
    run: str
    needs: list
    outputs: dict


def parse_and_validate_project_file(project_file):
    """Parse and validate the project file.

    Args:
        project_file: The contents of the project file as an immutable array of bytes.

    Returns:
        A dict representing the project.

    Raises:
        ProjectValidationError: The project could not be parsed, or was not valid
    """
    try:
        project = parse_yaml(project_file, name="project.yaml")
    except YAMLError as e:
        raise ProjectValidationError(*e.args)
    return validate_project_and_set_defaults(project)


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

        command, *args = shlex.split(action_config["run"])
        name, _, version = command.partition(":")
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
        raise UnknownActionError(f"Action '{action_id}' not found in project.yaml")
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
    )


def add_config_to_run_command(run_command, config):
    """Add --config flag to command.

    For commands that require complex config, users can supply a config key in
    project.yaml.  We serialize this as JSON, and pass it to the command with the
    --config flag.
    """
    config_as_json = json.dumps(config).replace("'", r"\u0027")
    return f"{run_command} --config '{config_as_json}'"


def requires_db_access(args):
    """
    By default actions do not have database access, but certain trusted actions require it
    """
    valid_commands = {
        "cohortextractor": ("generate_cohort", "generate_codelist_report"),
        "cohortextractor-v2": ("generate_cohort", "generate_dataset"),
        "databuilder": ("generate_dataset",),
    }
    if len(args) <= 1:
        return False

    image, command = args[0], args[1]
    image = image.split(":")[0]
    return command in valid_commands.get(image, [])


def is_generate_cohort_command(args, require_version=None):
    """
    The `cohortextractor generate_cohort` command gets special treatment in
    various places (e.g. it's the only command which gets access to the
    database) so it's helpful to have a single function for identifying it
    """
    assert not isinstance(args, str)
    version_found = None
    if len(args) > 1 and args[1] in ("generate_cohort", "generate_dataset"):
        if args[0].startswith("cohortextractor:"):
            version_found = 1
        # databuilder is a rebranded cohortextractor-v2.
        # Retain cohortextractor-v2 for backwards compatibility for now.
        elif args[0].startswith(("cohortextractor-v2:", "databuilder:")):
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
