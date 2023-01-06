import pytest

from jobrunner import lib


def test_atomic_writer_success(tmp_path):
    dst = tmp_path / "dst"
    with lib.atomic_writer(dst) as tmp:
        tmp.write_text("dst")

    assert dst.read_text() == "dst"
    assert not tmp.exists()


def test_atomic_writer_failure(tmp_path):
    dst = tmp_path / "dst"
    with pytest.raises(Exception):
        with lib.atomic_writer(dst) as tmp:
            tmp.write_text("dst")
            raise Exception("test")

    assert not dst.exists()
    assert not tmp.exists()


def test_atomic_writer_overwrite_symlink(tmp_path):
    target = tmp_path / "target"
    target.write_text("target")
    dst = tmp_path / "link"
    dst.symlink_to(target)

    with lib.atomic_writer(dst) as tmp:
        tmp.write_text("dst")

    assert dst.read_text() == "dst"
    assert not dst.is_symlink()
    assert target.read_text() == "target"
    assert not tmp.exists()


@pytest.mark.parametrize(
    "datestr, expected",
    [
        # docker datestrs, with and without ns
        ("2022-01-01T12:34:56.123456Z", 1641040496123456000),
        ("2022-01-01T12:34:56.123456789Z", 1641040496123456789),
        # busybox stat datestr, with and without ns
        ("2022-01-01 12:34:56.123456 +0000", 1641040496123456000),
        ("2022-01-01 12:34:56.123456789 +0000", 1641040496123456789),
        # short date
        ("2022-01-01T12:34:56", 1641040496000000000),
        # invalid
        ("not-a-timestamp", None),
        # check tz maths, just in case
        (
            "2022-01-01 12:34:56.123456789+02:00",
            1641040496123456789 - int(2 * 60 * 60 * 1e9),
        ),
    ],
)
def test_datestr_to_ns_timestamp(datestr, expected):
    assert lib.datestr_to_ns_timestamp(datestr) == expected
