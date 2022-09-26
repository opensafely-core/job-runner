import pytest

from jobrunner.lib.commands import requires_db_access


@pytest.mark.parametrize(
    "args",
    [
        ["cohortextractor:latest", "generate_cohort"],
        ["databuilder:latest", "generate-dataset"],
        ["cohortextractor:latest", "generate_codelist_report"],
        # Third and subsequent arguments are ignored:
        ["cohortextractor:latest", "generate_cohort", "could-be-anything-here"],
        # sqlrunner has an image but doesn't have a command
        ["sqlrunner:latest", "input.sql"],
    ],
)
def test_requires_db_access_privileged_commands_can_access_db(args):
    assert requires_db_access(args)


@pytest.mark.parametrize(
    "args",
    [
        # Only specific images get access
        ["python:latest", "script.py"],
        ["someotherimage:latest", "some-command"],
        # cohortextractor-v2 is no longer supported
        ["cohortextractor-v2:latest", "generate_dataset"],
        # Only the actual extractions commands get access
        ["cohortextractor:latest"],
        ["cohortextractor:latest", "some_other_command"],
        ["databuilder:latest"],
        ["databuilder:latest", "some-other-command"],
        # Check for command/image specificity
        ["cohortextractor:latest", "generate_dataset"],
        ["databuilder:latest", "generate_cohort"],
    ],
)
def test_requires_db_access_commands_cannot_access_db(args):
    assert not requires_db_access(args)
