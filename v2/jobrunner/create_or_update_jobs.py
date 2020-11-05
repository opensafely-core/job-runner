"""
This module provides a single public entry point `create_or_update_jobs`.

It handles all logic connected with creating or updating Jobs in response to
JobRequests. This includes fetching the code with git, validating the project
and doing the necessary dependency resolution.
"""
from .database import transaction, insert, exists_where, find_where
from .git import read_file_from_repo, get_sha_from_remote_ref, GitError
from .project import (
    parse_and_validate_project_file,
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
        # update_jobs(job_request)
        pass


def related_jobs_exist(job_request):
    return exists_where(Job, job_request_id=job_request.id)


def create_jobs(job_request):
    # In future I expect the job-server to only ever supply commits and so this
    # branch resolution will be redundant
    if not job_request.commit:
        job_request.commit = get_sha_from_remote_ref(
            job_request.repo_url, job_request.branch
        )
    project_file = read_file_from_repo(
        job_request.repo_url, job_request.commit, "project.yaml"
    )
    # Do most of the work in a separate functon which never needs to talk to
    # git, for easier testing
    create_jobs_with_project_file(job_request, project_file)


def create_jobs_with_project_file(job_request, project_file):
    project = parse_and_validate_project_file(project_file)
    with transaction():
        insert(SavedJobRequest(id=job_request.id, original=job_request.original))
        primary_job = recursively_add_jobs(
            job_request, project, job_request.action, force_run=job_request.force_run
        )
        # If we didn't create a job at all that means the outputs already exist
        # and we weren't forcing a run
        if not primary_job:
            raise JobRequestError("Outputs already exist")
        # If the returned job belongs to a different JobRequest that means we
        # just picked up an existing scheduled job and didn't create a new one
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

    action_spec = project["actions"][action_id]
    required_actions = action_spec.get("needs", [])

    # Get or create any required jobs
    required_jobs = [
        recursively_add_jobs(
            job_request,
            project,
            required_action,
            force_run=job_request.force_run_dependencies,
        )
        for required_action in required_actions
    ]
    wait_for_job_ids = [awaited_job.id for awaited_job in required_jobs if awaited_job]

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
        requires_outputs_from=required_actions,
        run_command=action_spec["run"],
        output_spec=action_spec["outputs"],
    )
    insert(job)
    return job


def create_failed_job(job_request, exception):
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
                error_message=f"{type(exception).__name__}: {exception}",
            ),
        )
