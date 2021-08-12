"""
This module provides a single public entry point `create_or_update_jobs`.

It handles all logic connected with creating or updating Jobs in response to
JobRequests. This includes fetching the code with git, validating the project
and doing the necessary dependency resolution.
"""
import logging
import re
import time
from pathlib import Path

from . import config
from .database import exists_where, find_where, insert, transaction, update_where
from .git import GitError, GitFileNotFoundError, read_file_from_repo
from .github_validators import (
    GithubValidationError,
    validate_branch_and_commit,
    validate_repo_url,
)
from .manage_jobs import action_has_successful_outputs
from .models import Job, SavedJobRequest, State
from .project import (
    RUN_ALL_COMMAND,
    ProjectValidationError,
    get_action_specification,
    get_all_actions,
    parse_and_validate_project_file,
)

# Placeholder to mark jobs that don't need doing
NULL_JOB = object()

log = logging.getLogger(__name__)


class JobRequestError(Exception):
    pass


class NothingToDoError(JobRequestError):
    pass


def create_or_update_jobs(job_request):
    """
    Create or update Jobs in response to a JobRequest

    Note that where there is an error with the JobRequest it will create a
    single, failed job with the error details rather than raising an exception.
    This allows the error to be synced back to the job-server where it can be
    displayed to the user.
    """
    if not related_jobs_exist(job_request):
        try:
            log.info(f"Handling new JobRequest:\n{job_request}")
            new_job_count = create_jobs(job_request)
            log.info(f"Created {new_job_count} new jobs")
        except (
            GitError,
            GithubValidationError,
            ProjectValidationError,
            JobRequestError,
        ) as e:
            log.info(f"JobRequest failed:\n{e}")
            create_failed_job(job_request, e)
        except Exception:
            log.exception("Uncaught error while creating jobs")
            create_failed_job(job_request, JobRequestError("Internal error"))
    else:
        if job_request.cancelled_actions:
            log.debug("Cancelling actions: %s", job_request.cancelled_actions)
            update_where(
                Job,
                {"cancelled": True},
                job_request_id=job_request.id,
                action__in=job_request.cancelled_actions,
            )
        else:
            log.debug("Ignoring already processed JobRequest")


def related_jobs_exist(job_request):
    return exists_where(Job, job_request_id=job_request.id)


def create_jobs(job_request):
    validate_job_request(job_request)
    try:
        if not config.LOCAL_RUN_MODE:
            project_file = read_file_from_repo(
                job_request.repo_url, job_request.commit, "project.yaml"
            )
        else:
            project_file = (Path(job_request.repo_url) / "project.yaml").read_bytes()
    except (GitFileNotFoundError, FileNotFoundError):
        raise JobRequestError(f"No project.yaml file found in {job_request.repo_url}")
    # Do most of the work in a separate function which never needs to talk to
    # git, for easier testing
    return create_jobs_with_project_file(job_request, project_file)


def create_jobs_with_project_file(job_request, project_file):
    project = parse_and_validate_project_file(project_file)

    active_jobs = get_active_jobs_for_workpace(job_request.workspace)
    new_jobs = get_jobs_to_run(job_request, project, active_jobs)

    if not new_jobs:
        if active_jobs:
            raise JobRequestError("All requested actions were already scheduled to run")
        else:
            raise NothingToDoError()

    # There is a delay between getting the active jobs from the database, and
    # getting the state of completed jobs from disk, and creating our new jobs
    # below. This means the state of the world may have changed in the
    # meantime. Why is this OK?
    #
    # Because we're single threaded and because this function is the only place
    # jobs are created, we can guarantee that no *new* jobs were created. So
    # the only state change that's possible is that some active jobs might have
    # completed. That's unproblematic: any jobs which were waiting on these
    # now-already-completed jobs will see they have completed the first time
    # they check and then proceed as normal.
    #
    # (It is also possible that someone could delete files off disk that are
    # needed by a particular job, but there's not much we can do about that
    # other than fail gracefully when trying to start the job.)
    with transaction():
        insert(SavedJobRequest(id=job_request.id, original=job_request.original))
        for job in new_jobs:
            insert(job)

    return len(new_jobs)


def get_active_jobs_for_workpace(workspace):
    return find_where(
        Job,
        workspace=workspace,
        state__in=[State.PENDING, State.RUNNING],
    )


def get_jobs_to_run(job_request, project, active_jobs):
    """
    Returns a list of jobs to run in response to the supplied JobReqeust

    Args:
        job_request: JobRequest instance
        project: dict representing the parsed project file from the JobRequest
        active_jobs: list of all pending or running jobs in the JobRequest's
            workspace
    """
    # Handle the special `run_all` action
    if RUN_ALL_COMMAND in job_request.requested_actions:
        actions_to_run = get_all_actions(project)
    else:
        actions_to_run = job_request.requested_actions

    # Build a dict mapping action names to job instances
    jobs_by_action = {job.action: job for job in active_jobs}
    # Add new jobs to it by recursing through the dependency tree
    for action in actions_to_run:
        recursively_build_jobs(jobs_by_action, job_request, project, action)

    # Pick out the new jobs we've added and return them
    new_jobs = [
        job
        for job in jobs_by_action.values()
        if job is not NULL_JOB and job not in active_jobs
    ]
    return new_jobs


def recursively_build_jobs(jobs_by_action, job_request, project, action):
    """
    Recursively populate the `jobs_by_action` dict with jobs

    Args:
        jobs_by_action: A dict mapping action ID strings to Job instances
        job_request: An instance of JobRequest representing the job request.
        project: A dict representing the project.
        action: The string ID of the action to be added as a job.
    """
    # If there's already a job scheduled for this action there's nothing to do
    if action in jobs_by_action:
        return

    # If the action doesn't need running create an emtpy placeholder job entry
    if not action_needs_running(job_request, action):
        jobs_by_action[action] = NULL_JOB
        return

    action_spec = get_action_specification(project, action)

    # Walk over the dependencies of this action, creating any necessary jobs,
    # and ensure that this job waits for its dependencies to finish before it
    # starts
    wait_for_job_ids = []
    for required_action in action_spec.needs:
        recursively_build_jobs(jobs_by_action, job_request, project, required_action)
        required_job = jobs_by_action[required_action]
        if required_job is not NULL_JOB:
            wait_for_job_ids.append(required_job.id)

    job = Job(
        job_request_id=job_request.id,
        state=State.PENDING,
        repo_url=job_request.repo_url,
        commit=job_request.commit,
        workspace=job_request.workspace,
        database_name=job_request.database_name,
        action=action,
        action_repo_url=action_spec.repo_url,
        action_commit=action_spec.commit,
        wait_for_job_ids=wait_for_job_ids,
        requires_outputs_from=action_spec.needs,
        run_command=action_spec.run,
        output_spec=action_spec.outputs,
        created_at=int(time.time()),
        updated_at=int(time.time()),
    )

    # Add it to the dictionary of scheduled jobs
    jobs_by_action[action] = job


def action_needs_running(job_request, action):
    """
    Does this action need to be run as part of this job request?
    """
    # Explicitly requested actions always get run
    if action in job_request.requested_actions:
        return True
    # If it's not an explicilty requested action then it's a dependency, and if
    # we're forcing all dependencies to run then we need to run this one
    if job_request.force_run_dependencies:
        return True

    # Has this dependency been run previously?
    action_status = action_has_successful_outputs(job_request.workspace, action)

    # Yes, and it was successful
    if action_status is True:
        # So no need to run it again
        return False

    # Yes, and it failed
    elif action_status is False:
        # If we're not re-running failed jobs then this is an error condition
        if not job_request.force_run_failed:
            raise JobRequestError(
                f"{action} failed on a previous run and must be re-run"
            )
        # Otherwise, re-run it
        return True

    # No, it's not been run before
    elif action_status is None:
        # So run it now
        return True
    else:
        raise RuntimeError(f"Unhandled action_status: {action_status}")


def validate_job_request(job_request):
    if config.ALLOWED_GITHUB_ORGS and not config.LOCAL_RUN_MODE:
        validate_repo_url(job_request.repo_url, config.ALLOWED_GITHUB_ORGS)
    if not job_request.workspace:
        raise JobRequestError("Workspace name cannot be blank")
    if not job_request.requested_actions:
        raise JobRequestError("At least one action must be supplied")
    # In local run mode the workspace name is whatever the user's working
    # directory happens to be called, which we don't want or need to place any
    # restrictions on. Otherwise, as these are externally supplied strings that
    # end up as paths, we want to be much more restrictive.
    if not config.LOCAL_RUN_MODE:
        if re.search(r"[^a-zA-Z0-9_\-]", job_request.workspace):
            raise JobRequestError(
                "Invalid workspace name (allowed are alphanumeric, dash and underscore)"
            )

    if not config.USING_DUMMY_DATA_BACKEND:
        database_name = job_request.database_name
        valid_names = config.DATABASE_URLS.keys()

        if database_name not in valid_names:
            raise JobRequestError(
                f"Invalid database name '{database_name}', allowed are: "
                + ", ".join(valid_names)
            )

        if not config.DATABASE_URLS[database_name]:
            raise JobRequestError(
                f"Database name '{database_name}' is not currently defined "
                f"for backend '{config.BACKEND}'"
            )
    # If we're not restricting to specific Github organisations then there's no
    # point in checking the provenance of the supplied commit
    if config.ALLOWED_GITHUB_ORGS and not config.LOCAL_RUN_MODE:
        # As this involves talking to the remote git server we only do it at
        # the end once all other checks have passed
        validate_branch_and_commit(
            job_request.repo_url, job_request.commit, job_request.branch
        )


def create_failed_job(job_request, exception):
    """
    Sometimes we want to say to the job-server (and the user): your JobRequest
    was broken so we weren't able to create any jobs for it. But the only way
    for the job-runner to communicate back to the job-server is by creating a
    job. So this function creates a single job with the special action name
    "__error__", which starts in the FAILED state and whose status_message
    contains the error we wish to communicate.

    This is a bit of a hack, but it keeps the sync protocol simple.
    """
    # Special case for the NothingToDoError which we treat as a success
    if isinstance(exception, NothingToDoError):
        state = State.SUCCEEDED
        status_message = "All actions have already run"
        action = job_request.requested_actions[0]
    else:
        state = State.FAILED
        status_message = f"{type(exception).__name__}: {exception}"
        action = "__error__"
    with transaction():
        insert(SavedJobRequest(id=job_request.id, original=job_request.original))
        now = int(time.time())
        insert(
            Job(
                job_request_id=job_request.id,
                state=state,
                repo_url=job_request.repo_url,
                commit=job_request.commit,
                workspace=job_request.workspace,
                action=action,
                status_message=status_message,
                created_at=now,
                started_at=now,
                updated_at=now,
                completed_at=now,
            ),
        )
