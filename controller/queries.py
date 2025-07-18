import sqlite3
import time
from itertools import groupby
from operator import attrgetter

from opentelemetry import trace

from controller.lib.database import fast_find_where, find_one, find_where, upsert
from controller.models import Flag, SavedJobRequest, TestJob


tracer = trace.get_tracer("db")


def calculate_workspace_state(backend, workspace):
    """
    Return a list containing the most recent uncancelled job (if any) for each action in the workspace. We always
    ignore cancelled jobs when considering the historical state of the system. We also ignore jobs whose action is
    '__error__'; these are dummy jobs created only to help us communicate failure states back to the job-server (see
    create_or_update_jobs.create_failed_job()).
    """
    with tracer.start_as_current_span("calculate_workspace_state_db") as span:
        all_jobs = fast_find_where(
            TestJob, workspace=workspace, cancelled=False, backend=backend
        )
        span.set_attribute("job_count", len(all_jobs))
        span.set_attribute("job.workspace", workspace)
        span.set_attribute("job.backend", backend)

    with tracer.start_as_current_span("calculate_workspace_state_python") as span:
        span.set_attribute("job_count", len(all_jobs))
        span.set_attribute("job.workspace", workspace)
        span.set_attribute("job.backend", backend)
        latest_jobs = []
        for action, jobs in group_by(all_jobs, attrgetter("action")):
            if action == "__error__":
                continue
            ordered_jobs = sorted(jobs, key=attrgetter("created_at"), reverse=True)
            latest_jobs.append(ordered_jobs[0])

    return (all_jobs, latest_jobs)


def group_by(iterable, key):
    return groupby(sorted(iterable, key=key), key=key)


def get_flag(name, backend):
    """Get a flag for a backend from the db"""
    return find_one(Flag, id=name, backend=backend)


def get_flag_value(name, backend, *, default=None):
    """Get the current value of a flag for a backend, with a default"""
    # Note: fail gracefully if the flags table does not exist
    try:
        return get_flag(name, backend).value
    except ValueError:
        return default
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            return default
        raise  # pragma: no cover


def set_flag(name, value, backend, timestamp=None):
    """Set a flag for a backend to a value in the db."""
    # Note: table must exist to set flags

    # If it's already in the desired state, do nothing to avoid updating the
    # timestamp
    try:
        current = get_flag(name, backend)
    except ValueError:
        pass
    else:
        if current.value == value:
            return current

    if timestamp is None:
        timestamp = time.time()
    flag = Flag(name, value, backend, timestamp)
    upsert(flag, keys=("id", "backend"))
    return flag


def get_current_flags(backend):
    """Get all currently set flags for a backend"""
    return find_where(Flag, backend=backend)


def get_saved_job_request(job):
    try:
        return find_one(SavedJobRequest, id=job.job_request_id).original
    except ValueError:
        return {}
