from jobrunner.lib.string_utils import project_name_from_url


def test_project_name_from_url():
    assert project_name_from_url("https://github.com/opensafely/test1.git") == "test1"
    assert project_name_from_url("https://github.com/opensafely/test2/") == "test2"
    assert project_name_from_url("/some/local/path/test3/") == "test3"
    assert project_name_from_url("C:\\some\\windows\\path\\test4\\") == "test4"
