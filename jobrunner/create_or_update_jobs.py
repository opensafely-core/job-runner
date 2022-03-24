"""
This module provides a single public entry point `create_or_update_jobs`.

It handles all logic connected with creating or updating Jobs in response to
JobRequests. This includes fetching the code with git, validating the project
and doing the necessary dependency resolution.
"""
import logging
import re
import time

from jobrunner import config
from jobrunner.lib.database import exists_where, insert, transaction, update_where
from jobrunner.lib.git import GitError, GitFileNotFoundError, read_file_from_repo
from jobrunner.lib.github_validators import (
    GithubValidationError,
    validate_branch_and_commit,
    validate_repo_url,
)
from jobrunner.models import Job, SavedJobRequest, State
from jobrunner.project import (
    RUN_ALL_COMMAND,
    ProjectValidationError,
    get_action_specification,
    get_all_actions,
    parse_and_validate_project_file,
)
from jobrunner.queries import calculate_workspace_state
from jobrunner.reusable_actions import (
    ReusableActionError,
    resolve_reusable_action_references,
)


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
            ReusableActionError,
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
            set_cancelled_flag_for_actions(
                job_request.id, job_request.cancelled_actions
            )
        else:
            log.debug("Ignoring already processed JobRequest")


def create_jobs(job_request):
    # NOTE: Similar but non-identical logic is implemented for running jobs
    # locally in `jobrunner.cli.local_run.create_job_request_and_jobs`. If you
    # make changes below then consider what the appropriate corresponding
    # changes are for locally run jobs.
    validate_job_request(job_request)
    project_file = get_project_file(job_request)
    project = parse_and_validate_project_file(project_file)
    latest_jobs = get_latest_jobs_for_actions_in_project(job_request.workspace, project)
    new_jobs = get_new_jobs_to_run(job_request, project, latest_jobs)
    assert_new_jobs_created(new_jobs, latest_jobs)
    resolve_reusable_action_references(new_jobs)
    # There is a delay between getting the current jobs (which we fetch from
    # the database and the disk) and inserting our new jobs below. This means
    # the state of the world may have changed in the meantime. Why is this OK?
    #
    # Because we're single threaded and because this function is the only place
    # jobs are created, we can guarantee that no *new* jobs were created. So
    # the only state change that's possible is that some active jobs might have
    # completed. That's unproblematic: any new jobs which are waiting on these
    # now-already-completed jobs will see they have completed the first time
    # they check and then proceed as normal.
    #
    # (It is also possible that someone could delete files off disk that are
    # needed by a particular job, but there's not much we can do about that
    # other than fail gracefully when trying to start the job.)
    insert_into_database(job_request, new_jobs)
    return len(new_jobs)


def validate_job_request(job_request):
    if config.ALLOWED_GITHUB_ORGS:
        validate_repo_url(job_request.repo_url, config.ALLOWED_GITHUB_ORGS)
    if not job_request.requested_actions:
        raise JobRequestError("At least one action must be supplied")
    if not job_request.workspace:
        raise JobRequestError("Workspace name cannot be blank")
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
    if config.ALLOWED_GITHUB_ORGS:
        # As this involves talking to the remote git server we only do it at
        # the end once all other checks have passed
        validate_branch_and_commit(
            job_request.repo_url, job_request.commit, job_request.branch
        )


def get_project_file(job_request):
    try:
        return read_file_from_repo(
            job_request.repo_url, job_request.commit, "project.yaml"
        )
    except GitFileNotFoundError:
        raise JobRequestError(f"No project.yaml file found in {job_request.repo_url}")


def get_latest_jobs_for_actions_in_project(workspace, project):
    return [
        job
        for job in calculate_workspace_state(workspace)
        if job.action in get_all_actions(project)
    ]


def get_new_jobs_to_run(job_request, project, current_jobs):
    """
    Returns a list of new jobs to run in response to the supplied JobReqeust

    Args:
        job_request: JobRequest instance
        project: dict representing the parsed project file from the JobRequest
        current_jobs: list containing the most recent Job for each action in
            the workspace
    """
    # Build a dict mapping action names to job instances
    jobs_by_action = {job.action: job for job in current_jobs}
    # Add new jobs to it by recursing through the dependency tree
    for action in get_actions_to_run(job_request, project):
        recursively_build_jobs(jobs_by_action, job_request, project, action)

    # Pick out the new jobs we've added and return them
    return [job for job in jobs_by_action.values() if job not in current_jobs]


def get_actions_to_run(job_request, project):
    # Handle the special `run_all` action
    if RUN_ALL_COMMAND in job_request.requested_actions:
        return get_all_actions(project)
    else:
        return job_request.requested_actions


def recursively_build_jobs(jobs_by_action, job_request, project, action):
    """
    Recursively populate the `jobs_by_action` dict with jobs

    Args:
        jobs_by_action: A dict mapping action ID strings to Job instances
        job_request: An instance of JobRequest representing the job request.
        project: A dict representing the project.
        action: The string ID of the action to be added as a job.
    """
    existing_job = jobs_by_action.get(action)
    if existing_job and not job_should_be_rerun(job_request, existing_job):
        return

    action_spec = get_action_specification(project, action)

    # Walk over the dependencies of this action, creating any necessary jobs,
    # and ensure that this job waits for its dependencies to finish before it
    # starts
    wait_for_job_ids = []
    for required_action in action_spec.needs:
        recursively_build_jobs(jobs_by_action, job_request, project, required_action)
        required_job = jobs_by_action[required_action]
        if required_job.state in [State.PENDING, State.RUNNING]:
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
        updated_at=int(time.time()),
    )

    # Add it to the dictionary of scheduled jobs
    jobs_by_action[action] = job


def job_should_be_rerun(job_request, job):
    """
    Do we need to run the action referenced by this job again?
    """
    # Already running or about to run so don't start a new one
    if job.state in [State.PENDING, State.RUNNING]:
        return False
    # Explicitly requested actions always get re-run
    if job.action in job_request.requested_actions:
        return True
    # If it's not an explicilty requested action then it's a dependency, and if
    # we're forcing all dependencies to run then we need to run this one
    if job_request.force_run_dependencies:
        return True

    # Otherwise if it succeeded last time there's no need to run again
    if job.state == State.SUCCEEDED:
        return False
    # If it failed last time ...
    elif job.state == State.FAILED:
        # ... and we're forcing failed jobs to re-run then re-run it
        if job_request.force_run_failed:
            return True
        # Otherwise it's an error condition
        raise JobRequestError(
            f"{job.action} failed on a previous run and must be re-run"
        )
    else:
        raise ValueError(f"Invalid state: {job}")


def assert_new_jobs_created(new_jobs, current_jobs):
    if new_jobs:
        return

    pending = [job.action for job in current_jobs if job.state == State.PENDING]
    running = [job.action for job in current_jobs if job.state == State.RUNNING]
    succeeded = [job.action for job in current_jobs if job.state == State.SUCCEEDED]
    failed = [job.action for job in current_jobs if job.state == State.FAILED]

    # There are two legitimate reasons we can end up with no new jobs to run...
    if len(succeeded) == len(current_jobs):
        #  ...one is that the "run all" action was requested but everything has already run successfully
        log.info(
            f"run_all requested, but all jobs are already successful: {', '.join(succeeded)}"
        )
        raise NothingToDoError()

    if len(pending) + len(running) == len(current_jobs):
        # ...the other is that every requested action is already running or pending, this is considered a user error
        statuses = ", ".join(f"{job.action}({job.state.name})" for job in current_jobs)
        log.info(f"All requested actions were already scheduled to run: {statuses}")
        raise JobRequestError("All requested actions were already scheduled to run")

    # But if we get here then we've somehow failed to schedule new jobs despite the fact that some of the actions we
    # depend on have failed, which is a bug.
    raise Exception(
        f"Unexpected failed jobs in dependency graph after scheduling: {', '.join(failed)}"
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
    now = int(time.time())
    job = Job(
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
    )
    insert_into_database(job_request, [job])


def insert_into_database(job_request, jobs):
    with transaction():
        insert(SavedJobRequest(id=job_request.id, original=job_request.original))
        for job in jobs:
            insert(job)


def related_jobs_exist(job_request):
    return exists_where(Job, job_request_id=job_request.id)


def set_cancelled_flag_for_actions(job_request_id, actions):
    # It's important that we modify the Jobs in-place in the database rather than retrieving, updating and re-writing
    # them. If we did the latter then we would risk dirty writes if the run thread modified a Job while we were
    # working.
    update_where(
        Job,
        {"cancelled": True},
        job_request_id=job_request_id,
        action__in=actions,
    )
