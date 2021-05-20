"""
This module provides a single public entry point `create_or_update_jobs`.

It handles all logic connected with creating or updating Jobs in response to
JobRequests. This includes fetching the code with git, validating the project
and doing the necessary dependency resolution.
"""
import logging
from pathlib import Path
import re
from urllib.parse import urlparse
import time

from . import config
from .database import (
    transaction,
    insert,
    exists_where,
    find_where,
    count_where,
    update_where,
)
from .git import (
    read_file_from_repo,
    get_sha_from_remote_ref,
    commit_reachable_from_ref,
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
        updated_at=int(time.time()),
    )
    insert(job)
    return job


def validate_job_request(job_request):
    if config.ALLOWED_GITHUB_ORGS and not config.LOCAL_RUN_MODE:
        validate_repo_url(job_request.repo_url, config.ALLOWED_GITHUB_ORGS)
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


def validate_repo_url(repo_url, allowed_gitub_orgs):
    parsed_url = urlparse(repo_url)
    if parsed_url.scheme != "https" or parsed_url.netloc != "github.com":
        raise JobRequestError("Repository URLs must start https://github.com")
    path = parsed_url.path.strip("/").split("/")
    if not path or path[0] not in allowed_gitub_orgs:
        raise JobRequestError(
            f"Repositories must belong to one of the following Github "
            f"organisations: {' '.join(allowed_gitub_orgs)}"
        )
    expected_url = f"https://github.com/{'/'.join(path[:2])}"
    if repo_url.rstrip("/") != expected_url or len(path) != 2:
        raise JobRequestError(
            "Repository URL was not of the expected format: "
            "https://github.com/[organisation]/[project-name]"
        )


def validate_branch_and_commit(repo_url, commit, branch):
    """
    Due to the way Github works, anyone who can open a pull request against a
    repository can make a commit appear to be "in" that repository, even if
    they do not have write access to it.

    For example, someone created this PR against the Linux kernel:
    https://github.com/torvalds/linux/pull/437

    And even though this will never be merged, it still appears as a commit in
    that repo:
    https://github.com/torvalds/linux/commit/2793ae1df012c7c3f13ea5c0f0adb99017999c3b

    If we are enforcing that only code from certain organisations can be run
    then we need to check that any commits supplied have been made by someone
    with write access to the repository, which means we need to check they
    belong to a branch or tag in the repository.
    """
    if not branch:
        raise JobRequestError("A branch name must be supplied")
    # A further wrinkle is that each PR gets an associated ref within the repo
    # of the form `pull/PR_NUMBER/head`. So we enforce that the branch name
    # must be a "plain vanilla" branch name with no slashes.
    if "/" in branch:
        raise JobRequestError(f"Branch name must not contain slashes: {branch}")
    if not commit_reachable_from_ref(repo_url, commit, branch):
        raise JobRequestError(f"Could not find commit on branch '{branch}': {commit}")


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
