import os
import tempfile

from jobrunner.manage_jobs import delete_files


def is_filesystem_case_sensitive():
    """Returns True if the filesystem is case sensitive; otherwise returns False."""
    # Return a file-like object with some upper-case letters in its name that is deleted
    # as soon as it is closed.
    with tempfile.NamedTemporaryFile(prefix="TEMPORARY_FILE_") as temporary_file:
        # The name property contains the path to the file.
        return not os.path.exists(temporary_file.name.lower())


def test_delete_files(tmp_path):
    (tmp_path / "foo1").touch()
    (tmp_path / "foo2").touch()
    (tmp_path / "foo3").touch()
    delete_files(tmp_path, ["foo1", "foo2", "foo3"], files_to_keep=["FOO1", "foo2"])
    filenames = [f.name for f in tmp_path.iterdir()]
    expected = ["foo1", "foo2"] if not is_filesystem_case_sensitive() else ["foo2"]
    assert filenames == expected
