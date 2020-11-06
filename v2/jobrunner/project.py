import re
import shlex

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError, YAMLStreamError, YAMLWarning, YAMLFutureWarning

from . import config
from .os_utils import safe_join, UnsafePathError


# Build a config dict in the same format the old code expects
RUN_COMMANDS_CONFIG = {
    image: {"docker_invocation": [f"{config.DOCKER_REGISTRY}/{image}"]}
    for image in config.ALLOWED_IMAGES
}


class ProjectValidationError(Exception):
    pass


class ProjectYAMLError(ProjectValidationError):
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
    validate_project(project)
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


def validate_project(project):
    """Check that a dictionary of project actions is valid"""
    expected_version = project.get("version", None)
    if expected_version != "1.0":
        raise ProjectValidationError(
            "Project file must specify a valid version (currently only 1.0)"
        )
    seen_runs = set()
    seen_output_files = set()
    project_actions = project["actions"]

    for action_id, action_config in project_actions.items():
        parts = shlex.split(action_config["run"])
        if parts[0].startswith("cohortextractor"):
            if len(parts) > 1 and parts[1] == "generate_cohort":
                if len(action_config["outputs"]) != 1:
                    raise ProjectValidationError(
                        f"A `generate_cohort` action must have exactly one output; "
                        f"{action_id} had {len(action_config['outputs'])}"
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
                    f"{privacy_level} is not valid (must be one of "
                    f"{', '.join(permitted_privacy_levels)})"
                )

            for output_id, filename in output.items():
                try:
                    safe_join("/", filename)
                except UnsafePathError:
                    raise ProjectValidationError(
                        f"Output path {filename} is not permitted"
                    )
                if filename in seen_output_files:
                    raise ProjectValidationError(
                        f"Output path {filename} is not unique"
                    )
                seen_output_files.add(filename)
        # Check it's a permitted run command
        name, version, args = split_and_format_run_command(action_config["run"])
        if name not in RUN_COMMANDS_CONFIG:
            raise ProjectValidationError(f"{name} is not a supported command")
        if not version:
            raise ProjectValidationError(
                f"{name} must have a version specified (e.g. {name}:0.5.2)"
            )
        # Check the run command + args signature appears only once in
        # a project
        run_signature = f"{name}_{args}"
        if run_signature in seen_runs:
            raise ProjectValidationError(
                f"{name} {' '.join(args)} appears more than once"
            )
        seen_runs.add(run_signature)

        # Check any variables are supported
        for v in variables_in_string(action_config["run"]):
            if not v.replace(" ", "").startswith("${{needs"):
                raise ProjectValidationError(f"Unsupported variable {v}")
            try:
                _, action_id, outputs_key, privacy_level, output_id = v.split(".")
                if outputs_key != "outputs":
                    raise ProjectValidationError(f"Unable to find variable {v}")
            except ValueError:
                raise ProjectValidationError(f"Unable to find variable {v}")


def docker_args_from_run_command(run_command):
    run_token, version, args = split_and_format_run_command(run_command)
    docker_image = RUN_COMMANDS_CONFIG[run_token]["docker_invocation"][0]
    if version is None:
        version = "latest"
    return " ".join([f"{docker_image}:{version}"] + args)


def split_and_format_run_command(run_command):
    """A `run` command is in the form of `run_token:optional_version [args]`.

    Shell-split this into its constituent parts, with any substitution
    tokens normalized and escaped for later parsing and formatting.

    """
    for v in variables_in_string(run_command):
        # Remove spaces to prevent shell escaping from thinking these
        # are different tokens
        run_command = run_command.replace(v, v.replace(" ", ""))
        # Escape braces to prevent python `format()` from coverting
        # doubled braces in single ones
        run_command = escape_braces(run_command)

    parts = shlex.split(run_command)
    # Commands are in the form command:version
    if ":" in parts[0]:
        run_token, version = parts[0].split(":")
    else:
        run_token = parts[0]
        version = None

    return run_token, version, parts[1:]


def variables_in_string(string_with_variables, variable_name_only=False):
    """Return a list of variables of the form `${{ var }}` (or `${{var}}`)
    in the given string.

    Setting the `variable_name_only` flag will a list of variables of
    the form `var`

    """
    matches = re.findall(
        r"(\$\{\{ ?([A-Za-z][A-Za-z0-9.-_]+) ?\}\})", string_with_variables
    )
    if variable_name_only:
        return [x[1] for x in matches]
    else:
        return [x[0] for x in matches]


def escape_braces(unescaped_string):
    """Escape braces so that they will be preserved through a string
    `format()` operation

    """
    return unescaped_string.replace("{", "{{").replace("}", "}}")
