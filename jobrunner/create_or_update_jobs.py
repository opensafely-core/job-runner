"""
This module provides a single public entry point `create_or_update_jobs`.

It handles all logic connected with creating or updating Jobs in response to
JobRequests. This includes fetching the code with git, validating the project
and doing the necessary dependency resolution.
"""
from . import config
from .database import transaction, insert, exists_where, find_where
from .git import read_file_from_repo, get_sha_from_remote_ref, GitError
from .project import (
    parse_and_validate_project_file,
    get_action_specification,
    ProjectValidationError,
)
from .models import Job, SavedJobRequest, State
from .manage_jobs import outputs_exist


class JobRequestError(Exception):
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
            create_jobs(job_request)
        except (GitError, ProjectValidationError, JobRequestError) as e:
            create_failed_job(job_request, e)
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
        pass


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
    project_file = read_file_from_repo(
        job_request.repo_url, job_request.commit, "project.yaml"
    )
    # Do most of the work in a separate function which never needs to talk to
    # git, for easier testing
    create_jobs_with_project_file(job_request, project_file)


def create_jobs_with_project_file(job_request, project_file):
    project = parse_and_validate_project_file(project_file)
    with transaction():
        insert(SavedJobRequest(id=job_request.id, original=job_request.original))
        primary_job = recursively_add_jobs(
            job_request, project, job_request.action, force_run=job_request.force_run
        )
        # If the outputs already exist and we weren't forcing a run then we
        # won't create a job at all, which is an error
        if not primary_job:
            raise JobRequestError("Outputs already exist")
        # If the returned job belongs to a different JobRequest that means we
        # just picked up an existing scheduled job and didn't create a new one,
        # which is also an error
        elif primary_job.job_request_id != job_request.id:
            raise JobRequestError("Action is already scheduled to run")


def recursively_add_jobs(job_request, project, action_id, force_run=False):
    # Is there already an equivalent job scheduled to run?
    already_active_jobs = find_where(
        Job,
        workspace=job_request.workspace,
        action=action_id,
        status__in=[State.PENDING, State.RUNNING],
    )
    if already_active_jobs:
        return already_active_jobs[0]

    # Return an empty job if the outputs already exist and we're not forcing a
    # run
    if not force_run:
        if outputs_exist(job_request, action_id):
            return

    action_spec = get_action_specification(project, action_id)

    # Get or create any required jobs
    wait_for_job_ids = []
    for required_action in action_spec.needs:
        required_job = recursively_add_jobs(
            job_request,
            project,
            required_action,
            force_run=job_request.force_run_dependencies,
        )
        if required_job:
            wait_for_job_ids.append(required_job.id)

    job = Job(
        id=Job.new_id(),
        job_request_id=job_request.id,
        status=State.PENDING,
        repo_url=job_request.repo_url,
        commit=job_request.commit,
        workspace=job_request.workspace,
        database_name=job_request.database_name,
        action=action_id,
        wait_for_job_ids=wait_for_job_ids,
        requires_outputs_from=action_spec.needs,
        run_command=action_spec.run,
        output_spec=action_spec.outputs,
    )
    insert(job)
    return job


def validate_job_request(job_request):
    # TODO: Think about whether to validate the repo_url here. The job-server
    # should enforce that it's a repo from an allowed source, but we may want
    # to double check that here.  This should be a configurable check though,
    # so we can run against local repos in test/development.
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
    with transaction():
        insert(SavedJobRequest(id=job_request.id, original=job_request.original))
        insert(
            Job(
                id=Job.new_id(),
                job_request_id=job_request.id,
                status=State.FAILED,
                repo_url=job_request.repo_url,
                commit=job_request.commit,
                workspace=job_request.workspace,
                action=job_request.action,
                status_message=f"{type(exception).__name__}: {exception}",
            ),
        )
