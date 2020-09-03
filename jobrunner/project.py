import copy
import logging
import os
import re
import shlex

import networkx as nx
import yaml

from jobrunner.exceptions import ProjectValidationError
from jobrunner.utils import (
    all_output_paths_for_action,
    getlogger,
    make_volume_name,
    needs_run,
    safe_join,
)

logger = getlogger(__name__)
baselogger = logging.LoggerAdapter(logger, {"job_id": "-"})

# These numbers correspond to "levels" as described in our security
# documentation
PRIVACY_LEVEL_HIGH = 3
PRIVACY_LEVEL_MEDIUM = 4

# The keys of this dictionary are all the supported `run` commands in
# jobs
RUN_COMMANDS_CONFIG = {
    "cohortextractor": {
        "docker_invocation": ["docker.opensafely.org/cohortextractor"],
    },
    "stata-mp": {"docker_invocation": ["docker.opensafely.org/stata-mp"]},
    "r": {"docker_invocation": ["docker.opensafely.org/r"]},
}


def escape_braces(unescaped_string):
    """Escape braces so that they will be preserved through a string
    `format()` operation

    """
    return unescaped_string.replace("{", "{{").replace("}", "}}")


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


def load_and_validate_project(workdir):
    """Check that a dictionary of project actions is valid"""
    with open(os.path.join(workdir, "project.yaml"), "r") as f:
        project = yaml.safe_load(f)

    expected_version = project.get("version", None)
    if expected_version != "1.0":
        raise ProjectValidationError(
            "Project file must specify a valid version (currently only 1.0)"
        )
    seen_runs = []
    project_actions = project["actions"]
    for action_id, action_config in project_actions.items():
        # Check outputs are permitted
        for privacy_level, output in action_config["outputs"].items():
            permitted_privacy_levels = [
                "highly_sensitive",
                "moderately_sensitive",
                "minimally_sensitive",
            ]
            if privacy_level not in permitted_privacy_levels:
                raise ProjectValidationError(
                    f"{privacy_level} is not valid (must be one of {', '.join(permitted_privacy_levels)})"
                )

            for output_id, filename in output.items():
                try:
                    safe_join(workdir, filename)
                except AssertionError:
                    raise ProjectValidationError(
                        f"Output path {filename} is not permitted"
                    )
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
                f"{name} {' '.join(args)} appears more than once", report_args=True
            )
        seen_runs.append(run_signature)

        # Check any variables are supported
        for v in variables_in_string(action_config["run"]):
            if not v.replace(" ", "").startswith("${{needs"):
                raise ProjectValidationError(
                    f"Unsupported variable {v}", report_args=True
                )
            try:
                _, action_id, outputs_key, privacy_level, output_id = v.split(".")
                if outputs_key != "outputs":
                    raise ProjectValidationError(
                        f"Unable to find variable {v}", report_args=True
                    )
            except ValueError:
                raise ProjectValidationError(
                    f"Unable to find variable {v}", report_args=True
                )
    return project


def interpolate_variables(args, dependency_actions):
    """Given a list of arguments, each a single string token, replace any
    that are variables using a dotted lookup against the supplied
    dependencies dictionary

    """
    interpolated_args = []
    for arg in args:
        variables = variables_in_string(arg, variable_name_only=True)
        if variables:
            try:
                # at this point, the command string has been
                # shell-split into separate tokens, so there is only
                # ever a single variable to interpolate
                _, action_id, variable_kind, privacy_level, variable_id = variables[
                    0
                ].split(".")
                dependency_action = dependency_actions[action_id]
                dependency_outputs = dependency_action[variable_kind]
                privacy_level = dependency_outputs[privacy_level]
                filename = privacy_level[variable_id]
                if variable_kind == "outputs":
                    # When copying outputs into the workspace, we
                    # namespace them by action_id, to avoid filename
                    # clashes
                    arg = os.path.join(action_id, variable_id, filename)
                else:
                    raise ProjectValidationError(
                        "Only variables of kind `outputs` are currently supported",
                        report_args=True,
                    )
            except (KeyError, ValueError):
                raise ProjectValidationError(
                    f"No output corresponding to {arg} was found", report_args=True
                )
        interpolated_args.append(arg)
    return interpolated_args


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


def add_runtime_metadata(
    action_from_project,
    requested_action_id=None,
    workspace=None,
    callback_url=None,
    **kwargs,
):
    """Given a run command specified in project.yaml, validate that it is
    permitted, and return how it should be invoked for `docker run`

    Adds docker_invocation, privacy_level, database_url, and
    container_name to the `action` dict.

    """
    job_config = copy.deepcopy(kwargs)
    action_from_project = copy.deepcopy(action_from_project)
    job_config.update(action_from_project)
    job_config["action_id"] = requested_action_id

    command = job_config["run"]
    name, version, user_args = split_and_format_run_command(command)

    # Convert human-readable database name into DATABASE_URL
    if job_config["backend"] != "expectations":
        job_config["database_url"] = os.environ[
            f"{workspace['db'].upper()}_DATABASE_URL"
        ]
    info = copy.deepcopy(RUN_COMMANDS_CONFIG[name])

    # Convert the command name into a full set of arguments that can
    # be passed to `docker run`, but preserving user-defined variables
    # in the form `${{ variable }}` for interpolation later (after the
    # dependences have been walked)
    docker_image_name, *docker_args = info["docker_invocation"]
    if version:
        docker_image_name = f"{docker_image_name}:{version}"
    # Interpolate variables from the job_config into user-supplied
    # arguments. Currently, only `database_url` is useful.
    all_args = docker_args + user_args
    # Substitute database_url for expecations_population
    if all_args[0] == "generate_cohort":
        if job_config["backend"] == "expectations":
            all_args.append("--expectations-population=1000")
        else:
            all_args.append("--database-url={database_url}")
    all_args = [arg.format(**job_config) for arg in all_args]

    job_config["docker_invocation"] = [docker_image_name] + all_args

    # Other metadata required to run and/or debug containers
    job_config["callback_url"] = callback_url
    job_config["workspace"] = workspace
    job_config["container_name"] = make_container_name(
        make_volume_name(job_config) + "-" + "-".join(job_config["outputs"].keys())
    )
    job_config["output_locations"] = [
        safe_join(x[0], x[1]) for x in all_output_paths_for_action(job_config)
    ]
    job_config["needs_run"] = needs_run(job_config)

    return job_config


def parse_project_yaml(workdir, job_spec):
    """Given a checkout of an OpenSAFELY repo containing a `project.yml`,
    check the provided job can run, and if so, update it with
    information about how to run it in a docker container.

    If the job has unfinished dependencies, a DependencyNotFinished
    exception is raised.

    """
    project = load_and_validate_project(workdir)
    project_actions = project["actions"]
    requested_action_id = job_spec["action_id"]
    if requested_action_id not in project_actions:
        raise ProjectValidationError(requested_action_id)
    job_config = job_spec.copy()
    # Build dependency graph
    graph = nx.DiGraph()
    for action_id, action_config in project_actions.items():
        project_actions[action_id]["action_id"] = action_id
        graph.add_node(action_id)
        for dependency_id in action_config.get("needs", []):
            graph.add_node(dependency_id)
            graph.add_edge(dependency_id, action_id)
    sorted_graph = nx.algorithms.dag.topological_sort(graph)
    dependencies = nx.algorithms.dag.ancestors(graph, source=requested_action_id)
    sorted_dependencies = [x for x in sorted_graph if x in dependencies]

    # Compute runtime metadata for the job we're interested
    job_action = add_runtime_metadata(
        project_actions[requested_action_id],
        requested_action_id=requested_action_id,
        **job_config,
    )

    # Do the same thing for dependencies, and also assert that they've
    # completed by checking their expected output exists
    dependency_actions = {}
    inputs = []
    any_needs_run = False
    if not job_config["force_run_dependencies"]:
        job_config["force_run"] = False

    for dependency_action_id in sorted_dependencies:
        # Adds docker_invocation and output files locations to the
        # config
        action = add_runtime_metadata(
            project_actions[dependency_action_id],
            requested_action_id=dependency_action_id,
            **job_config,
        )
        # Add the inputs accrued from the previous dependencies
        action["inputs"] = inputs[:]
        action["docker_invocation"] = interpolate_variables(
            action["docker_invocation"], dependency_actions
        )
        action["needed_by_id"] = job_spec["pk"]
        if any_needs_run or action["needs_run"]:
            any_needs_run = True
            action["needs_run"] = True
        dependency_actions[dependency_action_id] = action
        inputs.extend(all_output_paths_for_action(action))
    if any_needs_run:
        job_action["needs_run"] = True
    job_action["inputs"] = inputs
    # Now interpolate user-provided variables into docker
    # invocation. This must happen after metadata has been added to
    # the dependencies, as variables can reference the ouputs of other
    # actions
    job_action["docker_invocation"] = interpolate_variables(
        job_action["docker_invocation"], dependency_actions
    )

    job_config.update(job_action)
    job_config["dependencies"] = dependency_actions
    return job_config


def make_container_name(input_string):
    """Convert `input_string` to a valid docker container name"""
    container_name = re.sub(r"[^a-zA-Z0-9]", "-", input_string)
    # Remove any leading dashes, as docker requires images begin with [:alnum:]
    if container_name.startswith("-"):
        container_name = container_name[1:]
    return container_name
