import json
import os
import subprocess
from datetime import datetime

from opentelemetry import trace


DEFAULT_TIMEOUT = 10
CONTAINER_METADATA = {}


def get_started_at(container: str, timeout: int) -> int:
    if container not in CONTAINER_METADATA:
        env = os.environ.copy()
        env["DOCKER_TZ"] = "UTC"
        cmd = ["docker", "inspect", "-f", "{{.Created}}", container]
        ps = subprocess.run(
            cmd, capture_output=True, check=True, timeout=timeout, env=env
        )
        CONTAINER_METADATA[container] = ps.stdout.strip().decode("utf8")

    return CONTAINER_METADATA[container]


def run_and_read_json_lines(cmd, timeout):
    ps = subprocess.run(cmd, capture_output=True, check=True, timeout=timeout)
    return [json.loads(line) for line in ps.stdout.splitlines()]


def get_job_stats(timeout=DEFAULT_TIMEOUT):
    stats_raw = run_and_read_json_lines(
        ["docker", "stats", "--no-stream", "--no-trunc", "--format", "{{json .}}"],
        timeout=timeout,
    )

    stats = {}
    seen = set()
    for row in stats_raw:
        if not row["Name"].startswith("os-job-"):
            continue

        container = row["Name"]
        job_id = _parse_job_id(container)
        started_at = get_started_at(container, timeout=timeout)
        stats[job_id] = {
            "cpu_percentage": float(row["CPUPerc"].rstrip("%")),
            "memory_used": _parse_size(row["MemUsage"].split()[0]),
            "container_id": row["Container"],
            "started_at": _docker_datestr_to_int_timestamp(started_at),
        }
        seen.add(container)

    # remove stale containers from cache
    for container in set(CONTAINER_METADATA.keys()) - seen:
        del CONTAINER_METADATA[container]

    # track it so we can check cache size
    trace.get_current_span().set_attribute(
        "container_cache_size", len(CONTAINER_METADATA)
    )

    return stats


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


def _parse_job_id(container_name):
    return container_name.removeprefix("os-job-")


def _docker_datestr_to_int_timestamp(ts):
    # strptime can only handle 6 fractional digits, docker returns 9. Strip the
    # last 3 digits and the trailing Z
    return int(datetime.strptime(ts[0:-4], "%Y-%m-%dT%H:%M:%S.%f").timestamp())
