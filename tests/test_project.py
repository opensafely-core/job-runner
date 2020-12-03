import pytest

from jobrunner.project import (
    parse_and_validate_project_file,
    ProjectValidationError,
    assert_valid_glob_pattern,
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
