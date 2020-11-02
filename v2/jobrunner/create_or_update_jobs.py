import uuid

from .database import transaction, insert, exists_where, find_where
from .git import read_file_from_repo, get_sha_from_remote_ref, GitError
from .project import (
    parse_and_validate_project_file,
    ProjectValidationError,
    docker_args_from_run_command,
)
from .models import Job, SavedJobRequest, State
from .manage_containers import outputs_exist


class JobRequestError(Exception):
    pass


def create_or_update_jobs(job_request):
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
        recursively_add_jobs(
            job_request, project, job_request.action, is_primary_action=True
        )
        insert(SavedJobRequest(id=job_request.id, original=job_request.original))


def recursively_add_jobs(job_request, project, action_id, is_primary_action=False):
    # Has the job already run?
    if outputs_exist(job_request.workspace, action_id):
        if is_primary_action:
            if not job_request.force_run and not job_request.force_run_dependencies:
                raise JobRequestError("Outputs already exist")
        else:
            if not job_request.force_run_dependencies:
                return

    # Is there already an equivalent job scheduled to run?
    already_active_jobs = find_where(
        Job,
        workspace=job_request.workspace,
        action=action_id,
        status__in=[State.PENDING, State.RUNNING],
    )
    if already_active_jobs:
        if is_primary_action:
            raise JobRequestError("Action is already scheduled to run")
        else:
            return already_active_jobs[0].id

    action_spec = project["actions"][action_id]
    required_actions = action_spec.get("needs", [])

    # Get the job IDs of any required jobs
    wait_for_job_ids = [
        recursively_add_jobs(job_request, project, required_action)
        for required_action in required_actions
    ]
    # Remove any None entries (these are for actions which have already
    # completed and so there is no associated job we need to wait for)
    wait_for_job_ids = list(filter(None, wait_for_job_ids))

    job = Job(
        id=str(uuid.uuid4()),
        job_request_id=job_request.id,
        status=State.PENDING,
        repo_url=job_request.repo_url,
        commit=job_request.commit,
        workspace=job_request.workspace,
        action=action_id,
        wait_for_job_ids=wait_for_job_ids,
        requires_outputs_from=required_actions,
        run_command=docker_args_from_run_command(action_spec["run"]),
        output_spec=action_spec["outputs"],
    )
    insert(job)
    return job.id


def create_failed_job(job_request, exception):
    with transaction():
        insert(SavedJobRequest(id=job_request.id, original=job_request.original))
        insert(
            Job(
                id=str(uuid.uuid4()),
                job_request_id=job_request.id,
                status=State.FAILED,
                repo_url=job_request.repo_url,
                commit=job_request.commit,
                workspace=job_request.workspace,
                action=job_request.action,
                error_message=f"{type(exception).__name__}: {exception}",
            ),
        )
