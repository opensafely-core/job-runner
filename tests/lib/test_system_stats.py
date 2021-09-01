import pytest

from jobrunner.lib.system_stats import get_system_stats


@pytest.mark.needs_docker
def test_get_system_stats():
    stats = get_system_stats()
    assert {
        "total_disk_space",
        "available_disk_space",
        "total_memory",
        "available_memory",
    }.issubset(stats.keys())
