"""
This module provides a single public entry point `create_or_update_jobs`.

It handles all logic connected with creating or updating Jobs in response to
JobRequests. This includes fetching the code with git, using the pipeline
library to validate the pipeline configuration, and doing the necessary
dependency resolution.
"""
import logging
import re
import time

from pipeline import RUN_ALL_COMMAND, ProjectValidationError, load_pipeline

from jobrunner import config, tracing
from jobrunner.actions import get_action_specification
from jobrunner.lib.database import exists_where, insert, transaction, update_where
from jobrunner.lib.git import GitError, GitFileNotFoundError, read_file_from_repo
from jobrunner.lib.github_validators import (
    GithubValidationError,
    validate_branch_and_commit,
    validate_repo_url,
)
from jobrunner.models import Job, SavedJobRequest, State, StatusCode
from jobrunner.queries import calculate_workspace_state
from jobrunner.reusable_actions import (
    ReusableActionError,
    resolve_reusable_action_references,
)


log = logging.getLogger(__name__)


class JobRequestError(Exception):
    pass


class StaleCodelistError(JobRequestError):
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
            create_job_from_exception(job_request, e)
        except Exception:
            log.exception("Uncaught error while creating jobs")
            create_job_from_exception(job_request, JobRequestError("Internal error"))
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
    pipeline_config = load_pipeline(project_file)
    latest_jobs = get_latest_jobs_for_actions_in_project(
        job_request.workspace, pipeline_config
    )
    new_jobs = get_new_jobs_to_run(job_request, pipeline_config, latest_jobs)
    assert_new_jobs_created(job_request, new_jobs, latest_jobs)
    resolve_reusable_action_references(new_jobs)

    # check for database actions in the new jobs, and raise an exception if
    # codelists are out of date
    assert_codelists_ok(job_request, new_jobs)

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
    # http prefix allows local git repos, useful for tests
    if job_request.repo_url.startswith("http") and config.ALLOWED_GITHUB_ORGS:
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


def get_latest_jobs_for_actions_in_project(workspace, pipeline_config):
    return [
        job
        for job in calculate_workspace_state(workspace)
        if job.action in pipeline_config.all_actions
    ]


def get_new_jobs_to_run(job_request, pipeline_config, current_jobs):
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
    for action in get_actions_to_run(job_request, pipeline_config):
        recursively_build_jobs(jobs_by_action, job_request, pipeline_config, action)

    # Pick out the new jobs we've added and return them
    return [job for job in jobs_by_action.values() if job not in current_jobs]


def get_actions_to_run(job_request, pipeline_config):
    # Handle the special `run_all` action
    if RUN_ALL_COMMAND in job_request.requested_actions:
        return pipeline_config.all_actions
    else:
        return job_request.requested_actions


def recursively_build_jobs(jobs_by_action, job_request, pipeline_config, action):
    """
    Recursively populate the `jobs_by_action` dict with jobs

    Args:
        jobs_by_action: A dict mapping action ID strings to Job instances
        job_request: An instance of JobRequest representing the job request.
        pipeline_config: A Pipeline instance representing the pipeline configuration.
        action: The string ID of the action to be added as a job.
    """
    existing_job = jobs_by_action.get(action)
    if existing_job and not job_should_be_rerun(job_request, existing_job):
        return

    action_spec = get_action_specification(
        pipeline_config,
        action,
        using_dummy_data_backend=config.USING_DUMMY_DATA_BACKEND,
    )

    # Walk over the dependencies of this action, creating any necessary jobs,
    # and ensure that this job waits for its dependencies to finish before it
    # starts
    wait_for_job_ids = []
    for required_action in action_spec.needs:
        recursively_build_jobs(
            jobs_by_action, job_request, pipeline_config, required_action
        )
        required_job = jobs_by_action[required_action]
        if required_job.state in [State.PENDING, State.RUNNING]:
            wait_for_job_ids.append(required_job.id)

    timestamp = time.time()
    job = Job(
        job_request_id=job_request.id,
        state=State.PENDING,
        status_code=StatusCode.CREATED,
        # time in nanoseconds
        status_code_updated_at=int(timestamp * 1e9),
        status_message="Created",
        repo_url=job_request.repo_url,
        commit=job_request.commit,
        workspace=job_request.workspace,
        database_name=job_request.database_name,
        requires_db=action_spec.action.is_database_action,
        action=action,
        wait_for_job_ids=wait_for_job_ids,
        requires_outputs_from=action_spec.needs,
        run_command=action_spec.run,
        output_spec=action_spec.outputs,
        created_at=int(timestamp),
        updated_at=int(timestamp),
    )
    tracing.initialise_trace(job)

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
    # If it failed last time, re-run it by default
    elif job.state == State.FAILED:
        return True
    else:
        raise ValueError(f"Invalid state: {job}")


def assert_new_jobs_created(job_request, new_jobs, current_jobs):
    if new_jobs:
        return

    # There are two legitimate reasons we can end up with no new jobs to run:

    # One is that the "run all" action was requested but everything has already run
    # successfully or is already running. We raise the special `NothingToDoError` which
    # is treated as a successful outcome because we've already done everything that was
    # requested.
    if RUN_ALL_COMMAND in job_request.requested_actions:
        raise NothingToDoError("All actions have already completed succesfully")

    # The other reason is that every requested action is already running or pending,
    # this is considered a user error.
    current_job_states = {job.action: job.state for job in current_jobs}
    requested_action_states = {
        current_job_states.get(action) for action in job_request.requested_actions
    }
    if requested_action_states <= {State.PENDING, State.RUNNING}:
        raise NothingToDoError("All requested actions were already scheduled to run")

    # But if we get here then we've somehow failed to schedule new jobs despite the fact
    # that some of the actions we depend on have failed, which is a bug.
    raise Exception(f"Unexpected job states after scheduling: {current_job_states}")


def assert_codelists_ok(job_request, new_jobs):
    if job_request.codelists_ok:
        return True
    for job in new_jobs:
        # Codelists are out of date; fail the entire job request if any job
        # requires database access
        if job.requires_db:
            raise StaleCodelistError(
                f"Codelists are out of date (required by action {job.action})"
            )


def create_job_from_exception(job_request, exception):
    """
    Sometimes we want to say to the job-server (and the user): your JobRequest
    was broken so we weren't able to create any jobs for it. But the only way
    for the job-runner to communicate back to the job-server is by creating a
    job. So this function creates a single job with the special action name
    "__error__", which starts in the FAILED state and whose status_message
    contains the error we wish to communicate.

    This is a bit of a hack, but it keeps the sync protocol simple.
    """
    action = "__error__"
    error = exception
    state = State.FAILED
    status_message = str(exception)

    # Special case for the NothingToDoError which we treat as a success
    if isinstance(exception, NothingToDoError):
        state = State.SUCCEEDED
        code = StatusCode.SUCCEEDED
        action = job_request.requested_actions[0]
        error = None
    # StaleCodelistError is a failure but not an INTERNAL_ERROR
    elif isinstance(exception, StaleCodelistError):
        code = StatusCode.STALE_CODELISTS
    else:
        code = StatusCode.INTERNAL_ERROR
        # include exception name in message to aid debugging
        status_message = f"{type(exception).__name__}: {exception}"

    now = time.time()
    job = Job(
        job_request_id=job_request.id,
        state=state,
        status_code=code,
        status_message=status_message,
        # time in nanoseconds
        status_code_updated_at=int(now * 1e9),
        repo_url=job_request.repo_url,
        commit=job_request.commit,
        workspace=job_request.workspace,
        action=action,
        created_at=int(now),
        started_at=int(now),
        updated_at=int(now),
        completed_at=int(now),
    )
    tracing.initialise_trace(job)
    insert_into_database(job_request, [job])
    tracing.record_final_state(job, job.status_code_updated_at, error=error)


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
        {
            "cancelled": True,
            "completed_at": int(time.time()),
        },
        job_request_id=job_request_id,
        action__in=actions,
    )
