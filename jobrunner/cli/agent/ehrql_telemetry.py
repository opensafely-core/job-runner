import argparse
import json
import os
import sys
from datetime import datetime

from opentelemetry import context, trace

from jobrunner.tracing import setup_default_tracing


def main():
    parser = argparse.ArgumentParser(description="db_telemetry")
    parser.add_argument("dataset", help="dataset to emit telemetry to")
    parser.add_argument(
        "metadata", help="path to metadata.json", type=argparse.FileType("r")
    )
    parser.add_argument(
        "file",
        nargs="?",
        type=argparse.FileType("r"),
        default=sys.stdin,
        help="Input file (default: stdin)",
    )

    args = parser.parse_args()

    # force our name to be used as dataset
    os.environ["OTEL_SERVICE_NAME"] = args.dataset
    setup_default_tracing()

    metadata = json.load(args.metadata)

    try:
        run(metadata, args.file)
    finally:
        if args.file != sys.stdin:
            args.file.close()


def run(metadata, file):
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

    for i, line in enumerate(file, 1):
        line = line.strip()
        if not line:
            continue

        try:
            query = json.loads(line)
            emit(tracer, attrs, query, end_time_ns)
            print(i)
        except json.JSONDecodeError:
            print(f"bad json in line {i}: {line}")

    root.end(end_time=end_time_ns)
    context.detach(token)


def docker_datestr_to_ns(ts):
    # Docker timestamps have ns precision and a Z. We strip the Z and reduce to
    # ms precision, as strptime can't handle either
    return int(
        datetime.strptime(ts[0:-4], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1_000_000_000
    )


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
        "job.cpu_peak": metadata["job_metrics"]["cpu_peak"],
        "job.cpu_mean": metadata["job_metrics"]["cpu_mean"],
        "job.mem_mb_peak": metadata["job_metrics"]["mem_mb_peak"],
        "job.mem_mb_mean": metadata["job_metrics"]["mem_mb_mean"],
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
    main()
