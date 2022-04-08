import pytest

from jobrunner.lib.commands import requires_db_access


@pytest.mark.parametrize(
    "args",
    [
        ["cohortextractor:latest", "generate_cohort", "--output-dir=outputs"],
        ["cohortextractor-v2:latest", "generate_dataset", "--output-dir=outputs"],
        ["databuilder:latest", "generate_dataset", "--output-dir=outputs"],
        ["cohortextractor:latest", "generate_codelist_report", "--output-dir=outputs"],
    ],
)
def test_requires_db_access_privileged_commands_can_access_db(args):
    assert requires_db_access(args)


@pytest.mark.parametrize(
    "args",
    [
        ["cohortextractor:latest"],
        ["cohortextractor:latest", "generate_dataset", "--output-dir=outputs"],
        ["python", "script.py", "--output-dir=outputs"],
    ],
)
def test_requires_db_access_commands_cannot_access_db(args):
    assert not requires_db_access(args)
