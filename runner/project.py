import copy
import logging
import networkx as nx
import os
import re
import requests
import shlex
import subprocess
import yaml

from runner.exceptions import DependencyFailed
from runner.exceptions import DependencyRunning
from runner.exceptions import ProjectValidationError
from runner.utils import all_output_paths_for_action
from runner.utils import getlogger
from runner.utils import get_auth
from runner.utils import safe_join


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
    "stata-mp": {"docker_invocation": ["docker.opensafely.org/stata-mp"],},
}


def get_latest_matching_job_from_queue(
    repo=None, db=None, tag=None, action_id=None, **kw
):
    job = {
        "backend": os.environ["BACKEND"],
        "repo": repo,
        "db": db,
        "tag": tag,
        "operation": action_id,
        "limit": 1,
    }
    response = requests.get(
        os.environ["JOB_SERVER_ENDPOINT"], params=job, auth=get_auth()
    )
    response.raise_for_status()
    results = response.json()["results"]
    return results[0] if results else None


def push_dependency_job_from_action_to_queue(action):
    job = {
        "backend": os.environ["BACKEND"],
        "repo": action["repo"],
        "db": action["db"],
        "tag": action["tag"],
        "operation": action["action_id"],
    }
    job["callback_url"] = action["callback_url"]
    job["needed_by"] = action["needed_by"]
    response = requests.post(
        os.environ["JOB_SERVER_ENDPOINT"], json=job, auth=get_auth()
    )
    response.raise_for_status()
    return response


def docker_container_exists(container_name):
    cmd = [
        "docker",
        "ps",
        "--filter",
        f"name={container_name}",
        "--quiet",
    ]
    result = subprocess.run(cmd, capture_output=True, encoding="utf8")
    return result.stdout != ""


def start_dependent_job_or_raise_if_unfinished(dependency_action):
    """Do the target output files for this job exist?  If not, raise an
    exception to prevent the dependent job from starting.

    `DependencyRunning` exceptions have special handling in the main
    loop so the dependent job can be retried as necessary

    """
    if not needs_run(dependency_action):
        return

    if docker_container_exists(dependency_action["container_name"]):
        raise DependencyRunning(
            f"Not started because dependency `{dependency_action['action_id']}` is currently running (as {dependency_action['container_name']})",
            report_args=True,
        )

    dependency_status = get_latest_matching_job_from_queue(**dependency_action)
    baselogger.info(
        "Got job %s from queue to match %s",
        dependency_status,
        dependency_action["action_id"],
    )
    if dependency_status:
        if dependency_status["completed_at"]:
            if dependency_status["status_code"] == 0:
                new_job = push_dependency_job_from_action_to_queue(dependency_action)
                raise DependencyRunning(
                    f"Not started because dependency `{dependency_action['action_id']}` has been added to the job queue at {new_job['url']} as its previous output can no longer be found",
                    report_args=True,
                )
            else:
                raise DependencyFailed(
                    f"Dependency `{dependency_action['action_id']}` failed, so unable to run this operation",
                    report_args=True,
                )

        elif dependency_status["started"]:
            raise DependencyRunning(
                f"Not started because dependency `{dependency_action['action_id']}` is just about to start",
                report_args=True,
            )
        else:
            raise DependencyRunning(
                f"Not started because dependency `{dependency_action['action_id']}` is waiting to start",
                report_args=True,
            )

    push_dependency_job_from_action_to_queue(dependency_action)
    raise DependencyRunning(
        f"Not started because dependency `{dependency_action['action_id']}` has been added to the job queue",
        report_args=True,
    )


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
    """Check that a dictionary of project actions is valid
    """
    with open(os.path.join(workdir, "project.yaml"), "r") as f:
        project = yaml.safe_load(f)

    expected_version = project.get("version", None)
    if expected_version != "1.0":
        raise ProjectValidationError(
            f"Project file must specify a valid version (currently only 1.0)"
        )
    seen_runs = []
    project_actions = project["actions"]
    for action_id, action_config in project_actions.items():
        # Check it's a permitted run command
        name, version, args = split_and_format_run_command(action_config["run"])
        if name not in RUN_COMMANDS_CONFIG:
            raise ProjectValidationError(name)
        if not version:
            raise ProjectValidationError(
                f"{name} must have a version specified (e.g. {name}:0.5.2)"
            )
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
        # Check the run command + args signature appears only once in
        # a project
        run_signature = f"{name}_{args}"
        if run_signature in seen_runs:
            raise ProjectValidationError(name, args, report_args=True)
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
                    arg = f"{action_id}_{filename}"
                else:
                    arg = filename
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
    action, repo=None, db=None, tag=None, callback_url=None, operation=None, **kwargs,
):
    """Given a run command specified in project.yaml, validate that it is
    permitted, and return how it should be invoked for `docker run`

    Adds docker_invocation, privacy_level, database_url, and
    container_name to the `action` dict.

    """
    action = copy.deepcopy(action)
    command = action["run"]
    name, version, user_args = split_and_format_run_command(command)

    # Convert human-readable database name into DATABASE_URL
    action["database_url"] = os.environ[f"{db.upper()}_DATABASE_URL"]
    info = copy.deepcopy(RUN_COMMANDS_CONFIG[name])

    # Convert the command name into a full set of arguments that can
    # be passed to `docker run`, but preserving user-defined variables
    # in the form `${{ variable }}` for interpolation later (after the
    # dependences have been walked)
    docker_image_name, *docker_args = info["docker_invocation"]
    if version:
        docker_image_name = f"{docker_image_name}:{version}"
    # Interpolate variables from the action into user-supplied
    # arguments. Currently, only `database_url` is useful.
    all_args = docker_args + user_args
    all_args = [arg.format(**action) for arg in all_args]
    action["docker_invocation"] = [docker_image_name] + all_args

    # Other metadata required to run and/or debug containers
    action["container_name"] = make_container_name(
        f"{repo}{db}{tag}{action['outputs']}"
    )
    action["callback_url"] = callback_url
    action["repo"] = repo
    action["db"] = db
    action["tag"] = tag
    action["output_locations"] = [
        {"privacy_level": privacy_level, "name": name, "location": path}
        for privacy_level, name, path in all_output_paths_for_action(action)
    ]
    action["needed_by"] = operation
    return action


def get_namespaced_outputs_from_dependencies(dependency_actions):
    """Given a list of dependencies, construct a dictionary that
    represents all the files these dependencies are expected to
    output. To avoid filename clashes, these are namespaced by the
    action that outputs the file.

    """
    inputs = {}
    for dependency in dependency_actions.values():
        for _, _, output_path in all_output_paths_for_action(dependency):
            # Namespace the output file with the action id, so that
            # copying files *into* the workspace doesn't overwrite
            # anything
            filename = os.path.basename(output_path)
            inputs[f"{dependency['action_id']}_{filename}"] = output_path
    return inputs


def parse_project_yaml(workdir, job_spec):
    """Given a checkout of an OpenSAFELY repo containing a `project.yml`,
    check the provided job can run, and if so, update it with
    information about how to run it in a docker container.

    If the job has unfinished dependencies, a DependencyNotFinished
    exception is raised.

    """
    project = load_and_validate_project(workdir)
    project_actions = project["actions"]
    requested_action_id = job_spec["operation"]
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
    dependencies = graph.predecessors(requested_action_id)

    # Compute runtime metadata for the job we're interested
    job_action = add_runtime_metadata(
        project_actions[requested_action_id], **job_config
    )

    # Do the same thing for dependencies, and also assert that they've
    # completed by checking their expected output exists
    dependency_actions = {}
    for action_id in dependencies:
        # Adds docker_invocation, privacy_level, and output_bucket to
        # the config
        action = add_runtime_metadata(project_actions[action_id], **job_config)
        start_dependent_job_or_raise_if_unfinished(action)
        dependency_actions[action_id] = action

    # Now interpolate user-provided variables into docker
    # invocation. This must happen after metadata has been added to
    # the dependencies, as variables can reference the ouputs of other
    # actions
    job_action["docker_invocation"] = interpolate_variables(
        job_action["docker_invocation"], dependency_actions
    )
    job_action["namespaced_inputs"] = get_namespaced_outputs_from_dependencies(
        dependency_actions
    )
    job_config.update(job_action)
    return job_config


def needs_run(action):
    return not all(
        os.path.exists(path) for _, _, path in all_output_paths_for_action(action)
    )


def make_container_name(input_string):
    """Convert `input_string` to a valid docker container name
    """
    container_name = re.sub(r"[^a-zA-Z0-9]", "-", input_string)
    # Remove any leading dashes, as docker requires images begin with [:alnum:]
    if container_name.startswith("-"):
        container_name = container_name[1:]
    return container_name
