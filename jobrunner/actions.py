import dataclasses
import json
import shlex

from pipeline.exceptions import ProjectValidationError
from pipeline.models import Action
from pipeline.outputs import get_output_dirs


class UnknownActionError(ProjectValidationError):
    pass


# Tiny dataclass to capture the specification of a project action
@dataclasses.dataclass
class ActionSpecification:
    run: str
    needs: list[str]
    outputs: dict[str, dict[str, str]]
    action: Action


def get_action_specification(config, action_id):
    """Get a specification for the action from the project.

    Args:
        config: A Pipeline model representing the pipeline configuration.
        action_id: The string ID of the action.

    Returns:
        An instance of ActionSpecification.

    Raises:
        UnknownActionError: The action was not found in the project.
        ProjectValidationError: The project was not valid.
    """
    try:
        action_spec = config.actions[action_id]
    except KeyError:
        raise UnknownActionError(f"Action '{action_id}' not found in project.yaml")

    # create a new version of the run.parts list so we can mutate it without
    # affecting the original which is a property
    run_parts = list(action_spec.run.parts)

    if action_spec.config:
        # For commands that require complex config, users can supply a config
        # key in project.yaml.  We serialize this as JSON, and pass it to the
        # command with the --config flag.
        run_parts += [
            "--config",
            json.dumps(action_spec.config).replace("'", r"\u0027"),
        ]

    # Special case handling for the `cohortextractor generate_cohort` command
    if is_cohortextractor_generate_cohort(run_parts):
        output_dirs = get_output_dirs(action_spec.outputs)

        if len(output_dirs) == 1:  # pragma: no branch
            # Automatically configure the cohortextractor to produce output in the
            # directory the `outputs` spec is expecting.
            run_parts.append(f"--output-dir={output_dirs[0]}")

    run_command = shlex.join(run_parts)

    return ActionSpecification(
        run=run_command,
        needs=action_spec.needs,
        outputs=action_spec.outputs.dict(),
        action=action_spec,
    )


def is_cohortextractor_generate_cohort(args):
    return (
        len(args) > 1
        and args[0].startswith("cohortextractor:")
        and args[1] == "generate_cohort"
    )
