import json

from jobrunner.lib.subprocess_utils import subprocess_run


def get_volume_and_container_sizes():
    response = subprocess_run(
        ["docker", "system", "df", "--verbose", "--format", "{{json .}}"],
        capture_output=True,
        check=True,
    )
    data = json.loads(response.stdout)
    volumes = {row["Name"]: _parse_size(row["Size"]) for row in data["Volumes"]}
    containers = {row["Names"]: _parse_size(row["Size"]) for row in data["Containers"]}
    return volumes, containers


def get_container_stats():
    response = subprocess_run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
        capture_output=True,
        check=True,
    )
    data = [json.loads(line) for line in response.stdout.splitlines()]
    return {
        row["Name"]: {
            "cpu_percentage": float(row["CPUPerc"].rstrip("%")),
            "memory_used": _parse_size(row["MemUsage"].split()[0]),
        }
        for row in data
    }


CONVERSIONS = {
    "B": 1,
    "KB": 10 ** 3,
    # Is this correct?
    "kB": 10 ** 3,
    "KiB": 2 ** 10,
    "MB": 10 ** 6,
    "MiB": 2 ** 20,
    "GB": 10 ** 9,
    "GiB": 2 ** 30,
    "TB": 10 ** 12,
    "TiB": 2 ** 40,
}


def _parse_size(size):
    units = size.lstrip("0123456789.-")
    value = float(size[: -len(units)])
    return int(value * CONVERSIONS[units])
