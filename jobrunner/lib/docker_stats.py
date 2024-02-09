import json

from jobrunner.lib.subprocess_utils import subprocess_run


DEFAULT_TIMEOUT = 10


# backport of 3.9s removeprefix
def removeprefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix) :]
    return s


def get_job_stats(timeout=DEFAULT_TIMEOUT):
    # TODO: add volume sizes
    return get_container_stats(DEFAULT_TIMEOUT)


def get_container_stats(timeout=DEFAULT_TIMEOUT):
    response = subprocess_run(
        ["docker", "stats", "--no-stream", "--no-trunc", "--format", "{{json .}}"],
        capture_output=True,
        check=True,
        timeout=timeout,
    )
    data = [json.loads(line) for line in response.stdout.splitlines()]
    return {
        removeprefix(row["Name"], "os-job-"): {
            "cpu_percentage": float(row["CPUPerc"].rstrip("%")),
            "memory_used": _parse_size(row["MemUsage"].split()[0]),
            "container_id": row["Container"],
        }
        for row in data
        if row["Name"].startswith("os-job-")
    }


CONVERSIONS = {
    "B": 1,
    "KB": 10**3,
    # Is this correct?
    "kB": 10**3,
    "KiB": 2**10,
    "MB": 10**6,
    "MiB": 2**20,
    "GB": 10**9,
    "GiB": 2**30,
    "TB": 10**12,
    "TiB": 2**40,
}


def _parse_size(size):
    units = size.lstrip("0123456789.-")
    value = float(size[: -len(units)])
    return int(value * CONVERSIONS[units])
