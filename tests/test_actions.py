import argparse
import shlex
import sys

import pytest
from pipeline.exceptions import ProjectValidationError
from pipeline.models import Pipeline

from jobrunner.actions import UnknownActionError, get_action_specification


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
def test_get_action_specification_databuilder_has_output_flag():
    config = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_dataset": {
                    "run": "databuilder:latest generate_dataset --output=output/dataset.csv",
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
        == "databuilder:latest generate_dataset --output=output/dataset.csv"
    )


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
def test_get_action_specification_for_cohortextractor_generate_cohort_action():
    config = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort",
                    "outputs": {"highly_sensitive": {"cohort": "output/input.csv"}},
                }
            },
        }
    )

    action_spec = get_action_specification(
        config, "generate_cohort", using_dummy_data_backend=True
    )

    assert (
        action_spec.run
        == """cohortextractor:latest generate_cohort --expectations-population=1000 --output-dir=output"""
    )


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
@pytest.mark.parametrize("image", ["cohortextractor-v2", "databuilder"])
def test_get_action_specification_for_databuilder_action(image):
    config = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1000},
            "actions": {
                "generate_cohort_v2": {
                    "run": f"{image}:latest generate_cohort --output=output/cohort.csv --dummy-data-file dummy.csv",
                    "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
                }
            },
        }
    )

    action_spec = get_action_specification(
        config, "generate_cohort_v2", using_dummy_data_backend=True
    )

    assert (
        action_spec.run
        == f"""{image}:latest generate_cohort --output=output/cohort.csv --dummy-data-file dummy.csv"""
    )


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
@pytest.mark.parametrize("image", ["cohortextractor-v2", "databuilder"])
def test_get_action_specification_for_databuilder_errors(image):
    config = Pipeline(
        **{
            "version": 3,
            "expectations": {"population_size": 1_000},
            "actions": {
                "generate_cohort_v2": {
                    "run": f"{image}:latest generate_cohort --output=output/cohort.csv",
                    "outputs": {"highly_sensitive": {"cohort": "output/cohort.csv"}},
                }
            },
        }
    )

    msg = "--dummy-data-file is required for a local run"
    with pytest.raises(ProjectValidationError, match=msg):
        get_action_specification(
            config,
            "generate_cohort_v2",
            using_dummy_data_backend=True,
        )


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
def test_get_action_specification_with_config():
    config = Pipeline(
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
def test_get_action_specification_with_dummy_data_file_flag(tmp_path):
    dummy_data_file = tmp_path / "test.csv"
    with dummy_data_file.open("w") as f:
        f.write("test")

    config = Pipeline(
        **{
            "version": 1,
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort",
                    "outputs": {"moderately_sensitive": {"cohort": "output/input.csv"}},
                    "dummy_data_file": dummy_data_file,
                }
            },
        }
    )

    action_spec = get_action_specification(
        config,
        "generate_cohort",
        using_dummy_data_backend=True,
    )

    expected = " ".join(
        [
            "cohortextractor:latest",
            "generate_cohort",
            f"--dummy-data-file={dummy_data_file}",
            "--output-dir=output",
        ]
    )
    assert action_spec.run == expected


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
def test_get_action_specification_without_dummy_data_file_flag(tmp_path):
    dummy_data_file = tmp_path / "test.csv"
    with dummy_data_file.open("w") as f:
        f.write("test")

    config = Pipeline(
        **{
            "version": 1,
            "actions": {
                "generate_cohort": {
                    "run": "cohortextractor:latest generate_cohort",
                    "outputs": {"moderately_sensitive": {"cohort": "output/input.csv"}},
                    "dummy_data_file": dummy_data_file,
                }
            },
        }
    )

    action_spec = get_action_specification(config, "generate_cohort")

    expected = "cohortextractor:latest generate_cohort --output-dir=output"
    assert action_spec.run == expected


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="ActionSpecification is only used to build commands for Docker",
)
def test_get_action_specification_with_unknown_action():
    config = Pipeline(
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
