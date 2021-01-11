"""
This module provides a single public entry point `create_or_update_jobs`.

It handles all logic connected with creating or updating Jobs in response to
JobRequests. This includes fetching the code with git, validating the project
and doing the necessary dependency resolution.
"""
import logging
from pathlib import Path
import re
import time

from . import config
from .database import transaction, insert, exists_where, find_where, count_where
from .git import (
    read_file_from_repo,
    get_sha_from_remote_ref,
    GitError,
    GitFileNotFoundError,
)
from .project import (
    parse_and_validate_project_file,
    get_action_specification,
    get_all_actions,
    assert_valid_actions,
    ProjectValidationError,
    RUN_ALL_COMMAND,
)
from .models import Job, SavedJobRequest, State
from .manage_jobs import action_has_successful_outputs


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
        except (GitError, ProjectValidationError, JobRequestError) as e:
            log.info(f"JobRequest failed:\n{e}")
            create_failed_job(job_request, e)
        except Exception:
            log.exception("Uncaught error while creating jobs")
            create_failed_job(job_request, JobRequestError("Internal error"))
    else:
        # TODO: think about what sort of updates we want to support
        # I think these are probably limited to:
        #  * cancel any pending jobs
        #  * cancel any pending jobs and kill any running ones
        #  * update the target commit SHA for any pending jobs (although cancel and
        #    resubmit would also work for this and would probably be simpler)
        #
        # This could be implemented by adding a boolean `cancel` column to the
        # job table. The run loop would check the value of this each time it
        # checks the state of the job and if it's set it would either call
        # `docker kill` on it (if it's running) or move it immediately to the
        # FAILED state (if it's still pending).
        log.debug("Ignoring already processed JobRequest")


def related_jobs_exist(job_request):
    return exists_where(Job, job_request_id=job_request.id)


def create_jobs(job_request):
    validate_job_request(job_request)
    # In future I expect the job-server to only ever supply commits and so this
    # branch resolution will be redundant
    if not job_request.commit:
        job_request.commit = get_sha_from_remote_ref(
            job_request.repo_url, job_request.branch
        )
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
    assert_valid_actions(project, job_request.requested_actions)
    # By default just the actions which have been explicitly requested are
    # forced to re-run, but if `force_run_dependencies` is set then any action
    # whose outputs we need will be forced to re-run
    force_run_actions = job_request.requested_actions
    # Handle the special `run_all` action (there's no need to specifically
    # identify "leaf node" actions, the effect is the same)
    if RUN_ALL_COMMAND in job_request.requested_actions:
        requested_actions = get_all_actions(project)
    else:
        requested_actions = job_request.requested_actions

    with transaction():
        insert(SavedJobRequest(id=job_request.id, original=job_request.original))
        new_job_scheduled = False
        jobs_being_run = False
        for action in requested_actions:
            job = recursively_add_jobs(job_request, project, action, force_run_actions)
            if job:
                jobs_being_run = True
                # If the returned job doesn't belong to the current JobRequest
                # that means we just picked up an existing scheduled job and
                # didn't create a new one, if it does match then it's a new job
                if job.job_request_id == job_request.id:
                    new_job_scheduled = True

        if jobs_being_run:
            if not new_job_scheduled:
                raise JobRequestError(
                    "All requested actions were already scheduled to run"
                )
        else:
            raise NothingToDoError()

    return count_where(Job, job_request_id=job_request.id)


def recursively_add_jobs(job_request, project, action, force_run_actions):
    # Is there already an equivalent job scheduled to run?
    already_active_jobs = find_where(
        Job,
        workspace=job_request.workspace,
        action=action,
        state__in=[State.PENDING, State.RUNNING],
    )
    if already_active_jobs:
        return already_active_jobs[0]

    # If we're not forcing this particular action to run...
    if not job_request.force_run_dependencies and action not in force_run_actions:
        action_status = action_has_successful_outputs(job_request.workspace, action)
        # If we have already have successful outputs for an action then return
        # an empty job as there's nothing to do
        if action_status is True:
            return
        # If the action has explicitly failed (and we're not automatically
        # re-running failed jobs) then raise an error
        elif action_status is False and not job_request.force_run_failed:
            raise JobRequestError(
                f"{action} failed on a previous run and must be re-run"
            )
        # Otherwise create the job as normal

    action_spec = get_action_specification(project, action)

    # Get or create any required jobs
    wait_for_job_ids = []
    for required_action in action_spec.needs:
        required_job = recursively_add_jobs(
            job_request, project, required_action, force_run_actions
        )
        if required_job:
            wait_for_job_ids.append(required_job.id)

    job = Job(
        job_request_id=job_request.id,
        state=State.PENDING,
        repo_url=job_request.repo_url,
        commit=job_request.commit,
        workspace=job_request.workspace,
        database_name=job_request.database_name,
        action=action,
        wait_for_job_ids=wait_for_job_ids,
        requires_outputs_from=action_spec.needs,
        run_command=action_spec.run,
        output_spec=action_spec.outputs,
        created_at=int(time.time()),
    )
    insert(job)
    return job


def validate_job_request(job_request):
    # TODO: Think about whether to validate the repo_url here. The job-server
    # should enforce that it's a repo from an allowed source, but we may want
    # to double check that here.  This should be a configurable check though,
    # so we can run against local repos in test/development.
    if not job_request.workspace:
        raise JobRequestError("Workspace name cannot be blank")
    # In local run mode the workspace name is whatever the user's working
    # directory happens to be called, which we don't want or need to place any
    # restrictions on. Otherwise, as these are externally supplied strings that
    # end up as paths, we want to be much more restrictive.
    if not config.LOCAL_RUN_MODE:
        if re.search(r"[^a-zA-Z0-9_\-]", job_request.workspace):
            raise JobRequestError(
                "Invalid workspace name (allowed are alphanumeric, dash and underscore)"
            )
    database_name = job_request.database_name
    if config.USING_DUMMY_DATA_BACKEND:
        valid_names = ["dummy"]
    else:
        valid_names = config.DATABASE_URLS.keys()
    if database_name not in valid_names:
        raise JobRequestError(
            f"Invalid database name '{database_name}', allowed are: "
            + ", ".join(valid_names)
        )
    if not config.USING_DUMMY_DATA_BACKEND and not config.DATABASE_URLS[database_name]:
        raise JobRequestError(
            f"Database name '{database_name}' is not currently defined "
            f"for backend '{config.BACKEND}'"
        )


def create_failed_job(job_request, exception):
    """
    Sometimes we want to say to the job-server (and the user): your JobRequest
    was broken so we weren't able to create any jobs for it. But the only way
    for the job-runner to communicate back to the job-server is by creating a
    job. So this function creates a single job, which starts in the FAILED
    state and whose status_message contains the error we wish to communicate.
    """
    # Special case for the NothingToDoError which we treat as a success
    if isinstance(exception, NothingToDoError):
        state = State.SUCCEEDED
        status_message = "All actions have already run"
    else:
        state = State.FAILED
        status_message = f"{type(exception).__name__}: {exception}"
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
                action=job_request.requested_actions[0],
                status_message=status_message,
                created_at=now,
                started_at=now,
                updated_at=now,
                completed_at=now,
            ),
        )
