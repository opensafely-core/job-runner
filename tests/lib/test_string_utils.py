import pytest

from jobrunner.lib.string_utils import project_name_from_url, slugify, tabulate


@pytest.mark.parametrize(
    "input_string,slug",
    [
        ("string", "string"),
        ("neko猫", "neko"),
        ("string!@#$%^&**()", "string"),
        ("string_______string-------string string", "string-string-string-string"),
        ("__string__", "string"),
    ],
)
def test_slugify(input_string, slug):
    assert slugify(input_string) == slug


def test_project_name_from_url():
    assert project_name_from_url("https://github.com/opensafely/test1.git") == "test1"
    assert project_name_from_url("https://github.com/opensafely/test2/") == "test2"
    assert project_name_from_url("/some/local/path/test3/") == "test3"
    assert project_name_from_url("C:\\some\\windows\\path\\test4\\") == "test4"


@pytest.mark.parametrize(
    "rows,formatted_output",
    [
        ([], ""),
        ([["one", "two"], ["three", "four"]], "one   two \nthree four"),
        (
            [["verylongword", "b"], ["yeahyeahyeah", "猫猫猫"]],
            "verylongword b  \nyeahyeahyeah 猫猫猫",
        ),
    ],
)
def test_tabulate(rows, formatted_output):
    assert tabulate(rows) == formatted_output
