from itertools import groupby
from operator import attrgetter

from jobrunner.lib.database import find_where
from jobrunner.models import Job


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
