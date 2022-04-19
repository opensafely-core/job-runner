import pytest

from jobrunner.lib import atomic_writer


def test_atomic_writer_success(tmp_path):
    dst = tmp_path / "dst"
    with atomic_writer(dst) as tmp:
        tmp.write_text("dst")

    assert dst.read_text() == "dst"
    assert not tmp.exists()


def test_atomic_writer_failure(tmp_path):
    dst = tmp_path / "dst"
    with pytest.raises(Exception):
        with atomic_writer(dst) as tmp:
            tmp.write_text("dst")
            raise Exception("test")

    assert not dst.exists()
    assert not tmp.exists()


def test_atomic_writer_overwrite_symlink(tmp_path):
    target = tmp_path / "target"
    target.write_text("target")
    dst = tmp_path / "link"
    dst.symlink_to(target)

    with atomic_writer(dst) as tmp:
        tmp.write_text("dst")

    assert dst.read_text() == "dst"
    assert not dst.is_symlink()
    assert target.read_text() == "target"
    assert not tmp.exists()
