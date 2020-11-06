from pathlib import Path
import shlex
from types import SimpleNamespace

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError, YAMLStreamError, YAMLWarning, YAMLFutureWarning

from . import config
from .path_utils import assert_is_safe_path, UnsafePathError


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
    pass


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
        exc.context_mark.name = "project.yaml"
        exc.problem_mark.name = "project.yaml"
        exc.note = ""
        exc.warn = ""
    except Exception:
        pass
    return exc


def validate_project_and_set_defaults(project):
    """Check that a dictionary of project actions is valid, and set any defaults"""
    feat = get_feature_flags_for_version(float(project["version"]))
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
                    assert_is_safe_path(filename)
                except UnsafePathError as e:
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

    return project


def get_action_specification(project, action_id):
    try:
        action_spec = project["actions"][action_id]
    except KeyError:
        raise UnknownActionError(f"Action '{action_id}' not found in project.yaml")
    run_command = action_spec["run"]
    # Specical case handling for the `cohortextractor generate_cohort` command
    if is_generate_cohort_command(shlex.split(run_command)):
        if config.USING_DUMMY_DATA_BACKEND:
            size = int(project["expectations"]["population_size"])
            run_command += f" --expectations-population={size}"
        output_dirs = get_output_dirs(action_spec["outputs"])
        if len(output_dirs) != 1:
            raise ProjectValidationError(
                f"generate_cohort command should produce output in only one "
                f"directory, found {output_dirs}"
            )
        run_command += f" --output-dir={output_dirs[0]}"
    return {
        "run": run_command,
        "needs": action_spec.get("needs", []),
        "outputs": action_spec["outputs"],
    }


def is_generate_cohort_command(args):
    assert not isinstance(args, str)
    if (
        len(args) > 1
        and args[0].startswith("cohortextractor:")
        and args[1] == "generate_cohort"
    ):
        return True
    return False


def get_output_dirs(output_spec):
    filenames = []
    for group in output_spec.values():
        filenames.extend(group.values())
    dirs = set(Path(filename).parent for filename in filenames)
    return list(dirs)


def get_feature_flags_for_version(version):
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
            f"Project file must specify a valid version (currently only "
            f"<= {max(FEATURE_FLAGS_BY_VERSION.values())})",
        )
    return feat
