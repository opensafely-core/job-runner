from jobrunner.lib.docker import (
    MANAGEMENT_CONTAINER_IMAGE,
    DockerDiskSpaceError,
    docker,
)


__all__ = ["get_system_stats", "DockerDiskSpaceError"]

# Populated by calls to `register_command` below
COMMANDS = []
PARSERS = []


def get_system_stats():
    separator = "____"
    command = f" && echo {separator} && ".join(COMMANDS)
    response = docker(
        ["run", "--rm", MANAGEMENT_CONTAINER_IMAGE, "sh", "-c", command],
        capture_output=True,
        check=True,
    )
    output = response.stdout.decode("ascii", "ignore")
    stats = {}
    chunks = output.split(f"\n{separator}\n")
    for parser, chunk in zip(PARSERS, chunks):
        stats.update(parser(chunk))
    return stats


def register_command(command):
    def register_parser(fn):
        COMMANDS.append(command)
        PARSERS.append(fn)
        return fn

    return register_parser


# `-b`: output in bytes
@register_command("free -b")
def parse_output_from_free(output):
    # Example:
    #
    #               total        used        free      shared  buff/cache   available
    # Mem:    16640380928  6100041728   161775616  1044062208 10378563584  9152172032
    # Swap:   17040404480  3095134208 13945270272

    # Add a `type` header because the output doesn't include one
    output = "type " + output.strip()
    for row in _parse_table(output):
        if row["type"] == "Mem:":
            return {
                "total_memory": int(row["total"]),
                "available_memory": int(row["available"]),
            }


# `-k`: output in kilobytes
# `-P`: output in POSIX format
@register_command("df -kP /")
def parse_output_from_df(output):
    # Example:
    #
    # Filesystem           1024-blocks    Used Available Capacity Mounted on
    # overlay              967482320 639067280 279246760  70% /

    rows = _parse_table(output)
    assert len(rows) == 1
    data = rows[0]
    assert data["Mounted"] == "/"
    return {
        "total_disk_space": int(data["1024-blocks"]) * 1024,
        "available_disk_space": int(data["Available"]) * 1024,
    }


# `-P ALL`: show results for all processors
# `1 1`: produce 1 report, after an interval of 1 second
@register_command("mpstat -P ALL 1 1")
def parse_output_from_mpstat(output):
    # Example:
    # Linux 5.4.0-66-generic (c30c70806105)	03/09/21	_x86_64_	(4 CPU)
    #
    # 11:02:35     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest   %idle
    # 11:02:36     all   22.31    0.25    7.27    1.50    0.00    1.75    0.00    0.00   66.92
    # 11:02:36       0   14.29    0.00    3.06    1.02    0.00    1.02    0.00    0.00   80.61
    # 11:02:36       1   20.62    0.00    7.22    5.15    0.00    0.00    0.00    0.00   67.01
    # 11:02:36       2   16.49    0.00    5.15    0.00    0.00    6.19    0.00    0.00   72.16
    # 11:02:36       3   36.27    0.00   12.75    0.00    0.00    0.00    0.00    0.00   50.98
    #
    # Average:     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest   %idle
    # Average:     all   22.31    0.25    7.27    1.50    0.00    1.75    0.00    0.00   66.92
    # Average:       0   14.29    0.00    3.06    1.02    0.00    1.02    0.00    0.00   80.61
    # Average:       1   20.62    0.00    7.22    5.15    0.00    0.00    0.00    0.00   67.01
    # Average:       2   16.49    0.00    5.15    0.00    0.00    6.19    0.00    0.00   72.16
    # Average:       3   36.27    0.00   12.75    0.00    0.00    0.00    0.00    0.00   50.98
    prefix = "Average:"
    output = "\n".join(
        line[len(prefix) :] for line in output.splitlines() if line.startswith(prefix)
    )
    rows = _parse_table(output)
    rows = [
        {
            key: float(value) if key.startswith("%") else value
            for (key, value) in row.items()
        }
        for row in rows
    ]
    return {"mpstat": rows}


def _parse_table(table_str):
    table = [line.split() for line in table_str.strip().splitlines()]
    header, rows = table[0], table[1:]
    return [dict(zip(header, row)) for row in rows]
