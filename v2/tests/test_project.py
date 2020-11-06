import pytest

from jobrunner.project import parse_and_validate_project_file, ProjectValidationError


def test_error_on_duplicate_keys():
    with pytest.raises(ProjectValidationError):
        parse_and_validate_project_file(
            """
        top_level:
            duplicate: 1
            duplicate: 2
        """
        )
