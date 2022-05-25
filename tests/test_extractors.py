import pytest

from jobrunner.extractors import is_extraction_command


@pytest.mark.parametrize(
    "require_version,args,desired_outcome",
    [
        (1, ["cohortextractor:latest", "generate_cohort"], True),
        (1, ["cohortextractor-v2:latest", "generate_cohort"], False),
        (1, ["databuilder:latest", "generate_dataset"], False),
        (2, ["cohortextractor:latest", "generate_cohort"], False),
        (2, ["cohortextractor-v2:latest", "generate_cohort"], True),
        (2, ["databuilder:latest", "generate_dataset"], True),
    ],
)
def test_is_extraction_command_with_version(args, require_version, desired_outcome):
    output = is_extraction_command(args, require_version=require_version)

    assert output == desired_outcome


@pytest.mark.parametrize(
    "args,desired_outcome",
    [
        (["cohortextractor:latest", "generate_cohort"], True),
        (["cohortextractor-v2:latest", "generate_cohort"], True),
        (["databuilder:latest", "generate_dataset"], True),
        (["test"], False),
        (["test", "generate_cohort"], False),
        (["test", "generate_dataset"], False),
    ],
)
def test_is_extraction_command_without_version(args, desired_outcome):
    assert is_extraction_command(args) == desired_outcome
