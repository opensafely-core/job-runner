import pytest

from jobrunner.project import (
    parse_and_validate_project_file,
    ProjectValidationError,
    assert_valid_glob_pattern,
    assert_valid_published_output,
    InvalidPatternError,
)


def test_error_on_duplicate_keys():
    with pytest.raises(ProjectValidationError):
        parse_and_validate_project_file(
            """
        top_level:
            duplicate: 1
            duplicate: 2
        """
        )


def test_assert_valid_glob_pattern():
    assert_valid_glob_pattern("foo/bar/*.txt")
    assert_valid_glob_pattern("foo")
    bad_patterns = [
        "/abs/path",
        "ends/in/slash/",
        "not//canonical",
        "path/../traversal",
        "c:/windows/absolute",
        "recursive/**/glob.pattern",
        "questionmark?",
        "/[square]brackets",
    ]
    for pattern in bad_patterns:
        with pytest.raises(InvalidPatternError):
            assert_valid_glob_pattern(pattern)


@pytest.mark.parametrize(
    "info,msg",
    [
        ({}, "missing field 'type'"),
        ({"type": "table"}, "missing field 'files'"),
        ({"type": "table", "files": []}, "missing field 'description'"),
        (
            {"type": "INVALID", "files": [], "description": "description"},
            "invalid type of 'INVALID'",
        ),
        (
            {"type": "table", "files": ["highly.csv"], "description": "description"},
            "file 'highly.csv' does not match a moderately_sensitive action output.",
        ),
        (
            {"type": "notebook", "files": ["output.csv"], "description": "description"},
            "file 'output.csv' has invalid extension for output type 'notebook'",
        ),
    ],
)
def test_assert_valid_published_output(info, msg):
    moderate_outputs = {"output.csv"}
    with pytest.raises(ProjectValidationError) as err:
        assert_valid_published_output("name", info, moderate_outputs)

    assert str(err.value).startswith("outputs_for_publication name: " + msg)
