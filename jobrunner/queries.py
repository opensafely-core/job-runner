import sqlite3
import time
from itertools import groupby
from operator import attrgetter

from jobrunner.lib.database import find_all, find_one, find_where, upsert
from jobrunner.models import Flag, Job


def calculate_workspace_state(workspace):
    """
    Return a list containing the most recent uncancelled job (if any) for each action in the workspace. We always
    ignore cancelled jobs when considering the historical state of the system. We also ignore jobs whose action is
    '__error__'; these are dummy jobs created only to help us communicate failure states back to the job-server (see
    create_or_update_jobs.create_failed_job()).
    """
    all_jobs = find_where(Job, workspace=workspace, cancelled=False)
    latest_jobs = []
    for action, jobs in group_by(all_jobs, attrgetter("action")):
        if action == "__error__":
            continue
        ordered_jobs = sorted(jobs, key=attrgetter("created_at"), reverse=True)
        latest_jobs.append(ordered_jobs[0])
    return latest_jobs


def group_by(iterable, key):
    return groupby(sorted(iterable, key=key), key=key)


def get_flag(name):
    """Get a flag from the db"""
    return find_one(Flag, id=name)


def get_flag_value(name, default=None):
    """Get the current value of a flag, with a default"""
    # Note: fail gracefully if the flags table does not exist
    # This means we don't need to worry about it in local_run.
    try:
        return get_flag(name).value
    except ValueError:
        return default
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            return default
        raise


def set_flag(name, value, timestamp=None):
    """Set a flag to a value in the db."""
    # Note: table must exist to set flags
    if timestamp is None:
        timestamp = time.time()
    flag = Flag(name, value, timestamp)
    upsert(flag)
    return flag


def get_current_flags():
    """Get all currently set flags"""
    return find_all(Flag)
