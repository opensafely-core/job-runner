import argparse
import json
import os
import sys
from pathlib import Path

from agent.cli.ehrql_telemetry import (
    convert_ehrql_logs,
    docker_datestr_to_ns,
    trace_queries,
)
from common.tracing import setup_default_tracing


def main(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Emit ehrql query data as telemetry from an ehrql log file generated outside of a standard job pipeline."
            "Reads the log file, and converts the query timing to otel, and sends them out."
        )
    )
    parser.add_argument(
        "dataset", help="Temporary Honeycomb dataset to emit telemetry to."
    )
    parser.add_argument("logfile_path", type=Path, help="path to log file.")
    parser.add_argument(
        "operation",
        choices=["generate-dataset", "generate-measures"],
        help="type of ehrql operation",
    )
    parser.add_argument("workspace", help="name of workspace")
    parser.add_argument("commit", help="Commit used to run this action")
    parser.add_argument("action", help="action that this log represents")
    parser.add_argument(
        "--attrs", nargs="+", help="additional attributes to trace, in k=v pairs"
    )

    args = parser.parse_args(argv)
    logfile_path = args.logfile_path

    raw_lines = convert_ehrql_logs(logfile_path)

    logs = []
    for line in raw_lines:
        line = line.strip()
        if not line:  # pragma: nocover
            continue
        try:
            logs.append(json.loads(line))
        except json.JSONDecodeError:
            print(f"bad json from ehrql: {line}")

    # force our name to be used as dataset
    os.environ["OTEL_SERVICE_NAME"] = args.dataset
    setup_default_tracing("agent")

    extra_attrs = args.attrs or []
    extra_attrs = [attr.partition("=")[::2] for attr in extra_attrs]

    attrs = {
        "workspace": args.workspace,
        "action": args.action,
        "commit": args.commit,
        **{k: v for k, v in extra_attrs},
    }

    run(logs, args.operation, attrs)


def run(queries, operation, attrs):
    # Get approx start/end times from first and last queries
    start_time_ns = docker_datestr_to_ns(queries[0]["start"])
    end_time_ns = docker_datestr_to_ns(queries[-1]["end"])

    operation = "ehrql." + operation

    trace_queries(queries, operation, start_time_ns, end_time_ns, attrs)


if __name__ == "__main__":
    main(sys.argv[1:])
