import argparse
import shlex
import sys

import pytest
from pipeline.models import Pipeline

from controller.actions import UnknownActionError, get_action_specification


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
def test_get_action_specification_ehrql_has_output_flag():
    config = Pipeline.build(
        **{
            "version": 3,
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_dataset": {
                    "run": "ehrql:v1 generate-dataset dataset.py --output=output/dataset.csv",
                    "outputs": {
                        "highly_sensitive": {
                            "cohort": "output/dataset.csv",
                            "cohort2": "output/input2.csv",
                        }
                    },
                },
            },
        }
    )

    action_spec = get_action_specification(config, "generate_dataset")

    assert (
        action_spec.run
        == "ehrql:v1 generate-dataset dataset.py --output=output/dataset.csv"
    )


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
def test_get_action_specification_with_config():
    config = Pipeline.build(
        **{
            "version": 3,
            "expectations": {"population_size": 1_000},
            "actions": {
                "my_action": {
                    "run": "python:latest python action/__main__.py output/input.csv",
                    "config": {"option": "value"},
                    "outputs": {
                        "moderately_sensitive": {"my_figure": "output/my_figure.png"}
                    },
                }
            },
        }
    )

    action_spec = get_action_specification(config, "my_action")

    assert (
        action_spec.run
        == """python:latest python action/__main__.py output/input.csv --config '{"option": "value"}'"""
    )

    # Does argparse accept options after arguments?
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")  # option
    parser.add_argument("input_files", nargs="*")  # argument

    # If parser were in __main__.py, then parser.parse_args would receive sys.argv
    # by default. sys.argv[0] is the script name (either with or without a path,
    # depending on the OS) so we slice obs_run_command to mimic this.
    parser.parse_args(shlex.split(action_spec.run)[2:])


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
def test_get_action_specification_with_unknown_action():
    config = Pipeline.build(
        **{
            "version": 1,
            "actions": {
                "known_action": {
                    "run": "python:latest python test.py",
                    "outputs": {"moderately_sensitive": {"cohort": "output/input.csv"}},
                }
            },
        }
    )
    msg = "Action 'unknown_action' not found in project.yaml"
    with pytest.raises(UnknownActionError, match=msg):
        get_action_specification(config, "unknown_action")
