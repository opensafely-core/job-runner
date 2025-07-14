import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from opentelemetry import context, trace

from agent.executors.local import job_metadata_path
from common.tracing import setup_default_tracing


def main(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Emit an ehrql job's query data as telemetry."
            "Reads the job's log file, and converts the query timing to otel, and sends them out."
        )
    )
    parser.add_argument(
        "dataset", help="Temporary Honeycomb dataset to emit telemetry to."
    )
    parser.add_argument(
        "job", help="job to emit telemetry for. Either job id or path to job log dir."
    )
    args = parser.parse_args(argv)

    if os.path.isdir(args.job):
        log_dir = Path(args.job)
    else:
        log_dir = job_metadata_path(args.job).parent

    metadata_path = log_dir / "metadata.json"
    logfile_path = log_dir / "logs.txt"
    metadata = json.loads(metadata_path.read_text())

    assert "ehrql" in metadata["container_metadata"]["Config"]["Image"], (
        "Cowardly refusing to emit telemetry for non-ehrql job"
    )

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

    run(metadata, logs)


def run(metadata, queries):
    tracer = trace.get_tracer("queries")

    # get the start/end times from the docker container metadata
    container = metadata["container_metadata"]
    start_time_ns = docker_datestr_to_ns(container["State"]["StartedAt"])
    end_time_ns = docker_datestr_to_ns(container["State"]["FinishedAt"])
    operation = "ehrql." + container["Args"][0]  # e.g. 'ehrql.generate-dataset'

    attrs = get_attrs(metadata)
    root = tracer.start_span(
        operation, context={}, start_time=start_time_ns, attributes=attrs
    )

    # Set it as the active span
    ctx = trace.set_span_in_context(root)
    token = context.attach(ctx)

    for query in queries:
        emit(tracer, attrs, query, end_time_ns)

    root.end(end_time=end_time_ns)
    context.detach(token)


def docker_datestr_to_ns(ts):
    # Docker timestamps have ns precision and a Z. We strip the Z and reduce to
    # ms precision, as strptime can't handle either
    return int(
        datetime.strptime(ts[0:-4], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1_000_000_000
    )


def convert_ehrql_logs(logfile_path):  # pragma: nocover
    process = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--interactive",
            "ghcr.io/opensafely-core/ehrql:v1",
            "/app/scripts/parse_logs.py",
        ],
        input=logfile_path.read_text(),
        capture_output=True,
        text=True,
        check=True,
    )

    return process.stdout.splitlines()


def get_attrs(metadata):
    container = metadata["container_metadata"]
    return {
        "job.id": metadata["job_definition_id"],
        "task.id": metadata["task_id"],
        "job.exit_code": metadata["exit_code"],
        "job.oom_killed": metadata["oom_killed"],
        "job.cancelled": metadata["cancelled"],
        "job.workspace": container["Config"]["Labels"]["workspace"],
        "job.action": container["Config"]["Labels"]["action"],
        "job.run_command": " ".join(container["Args"]),
        # older metadata.json do not have job_metrics
        "job.cpu_peak": metadata["job_metrics"].get("cpu_peak", 0),
        "job.cpu_mean": metadata["job_metrics"].get("cpu_mean", 0),
        "job.mem_mb_peak": metadata["job_metrics"].get("mem_mb_peak", 0),
        "job.mem_mb_mean": metadata["job_metrics"].get("mem_mb_mean", 0),
    }


def emit(tracer, job_attrs, query, end_time_ns):
    name = query["name"]
    start_time_ns = docker_datestr_to_ns(query["start"])
    if query["end"]:
        end_time_ns = docker_datestr_to_ns(query["end"])
    attrs = job_attrs | query["attributes"]
    span = tracer.start_span(name, start_time=start_time_ns, attributes=attrs)
    span.end(end_time_ns)


if __name__ == "__main__":
    main(sys.argv[1:])
