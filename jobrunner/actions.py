import dataclasses
import json
import shlex
from typing import Dict, List

from pipeline.exceptions import ProjectValidationError
from pipeline.outputs import get_output_dirs

from .extractors import is_extraction_command


class UnknownActionError(ProjectValidationError):
    pass


# Tiny dataclass to capture the specification of a project action
@dataclasses.dataclass
class ActionSpecification:
    run: str
    needs: List[str]
    outputs: Dict[str, Dict[str, str]]


def get_action_specification(config, action_id, using_dummy_data_backend=False):
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
    if is_extraction_command(run_parts, require_version=1):
        # Set the size of the dummy data population, if that's what we're
        # generating.  Possibly this should be moved to the study definition
        # anyway, which would make this unnecessary.
        if using_dummy_data_backend:
            if action_spec.dummy_data_file is not None:
                run_parts.append(f"--dummy-data-file={action_spec.dummy_data_file}")
            else:
                size = config.expectations.population_size
                run_parts.append(f"--expectations-population={size}")

        output_dirs = get_output_dirs(action_spec.outputs)

        if len(output_dirs) == 1:
            # Automatically configure the cohortextractor to produce output in the
            # directory the `outputs` spec is expecting.
            run_parts.append(f"--output-dir={output_dirs[0]}")

    elif is_extraction_command(run_parts, require_version=2):
        # cohortextractor Version 2 expects all command line arguments to be
        # specified in the run command
        target = "--dummy-data-file"
        if using_dummy_data_backend and not any(
            arg == target or arg.startswith(f"{target}=") for arg in run_parts
        ):
            raise ProjectValidationError(
                "--dummy-data-file is required for a local run"
            )

    # TODO: we can probably remove this fork since the v1&2 forks cover it
    elif is_extraction_command(run_parts):  # pragma: no cover
        raise RuntimeError("Unhandled cohortextractor version")

    run_command = shlex.join(run_parts)

    return ActionSpecification(
        run=run_command,
        needs=action_spec.needs,
        outputs=action_spec.outputs.dict(exclude_unset=True),
    )
