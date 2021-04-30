import os

from jobrunner.manage_jobs import delete_files


def test_delete_files(tmp_path):
    (tmp_path / "foo1").touch()
    (tmp_path / "foo2").touch()
    (tmp_path / "foo3").touch()
    delete_files(tmp_path, ["foo1", "foo2", "foo3"], files_to_keep=["FOO1", "foo2"])
    filenames = [f.name for f in tmp_path.iterdir()]
    expected = ["foo1", "foo2"] if os.name == "nt" else ["foo2"]
    assert filenames == expected
