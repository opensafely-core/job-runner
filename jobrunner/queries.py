from itertools import groupby
from operator import attrgetter

from jobrunner.lib.database import find_where
from jobrunner.models import Job


def get_latest_job_for_each_action(workspace):
    """
    Return a list containing the most recent uncancelled job (if any) for each action in the workspace. We always
    ignore cancelled jobs when considering the historical state of the system.
    """
    all_jobs = find_where(Job, workspace=workspace, cancelled=False)
    latest_jobs = []
    for _, jobs in group_by(all_jobs, attrgetter("action")):
        ordered_jobs = sorted(jobs, key=attrgetter("created_at"), reverse=True)
        latest_jobs.append(ordered_jobs[0])
    return latest_jobs


def group_by(iterable, key):
    return groupby(sorted(iterable, key=key), key=key)
