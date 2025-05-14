import json
import os
import subprocess
from datetime import datetime


DEFAULT_TIMEOUT = 10


def run_and_read_json_lines(cmd, timeout):
    env = os.environ.copy()
    env["DOCKER_TZ"] = "UTC"
    ps = subprocess.run(cmd, capture_output=True, check=True, timeout=timeout, env=env)
    return [json.loads(line) for line in ps.stdout.splitlines()]


def get_job_stats(timeout=DEFAULT_TIMEOUT):
    stats_raw = run_and_read_json_lines(
        ["docker", "stats", "--no-stream", "--no-trunc", "--format", "{{json .}}"],
        timeout=timeout,
    )
    metadata_raw = run_and_read_json_lines(
        ["docker", "container", "ls", "--no-trunc", "--format", "{{json .}}"],
        timeout=timeout,
    )

    metadata_dict = {
        _parse_job_id(m["Names"]): m
        for m in metadata_raw
        if m["Names"].startswith("os-job-")
    }

    stats = {}
    for row in stats_raw:
        if not row["Name"].startswith("os-job-"):
            continue

        job_id = _parse_job_id(row["Name"])
        metadata = metadata_dict[job_id]

        stats[job_id] = {
            "cpu_percentage": float(row["CPUPerc"].rstrip("%")),
            "memory_used": _parse_size(row["MemUsage"].split()[0]),
            "container_id": row["Container"],
            "started_at": _docker_datestr_to_int_timestamp(metadata["CreatedAt"]),
        }

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
    # parsing timezone names is messy, e.g. BST, so just strip them
    return int(datetime.strptime(ts[:-4], "%Y-%m-%d %H:%M:%S %z").timestamp())
