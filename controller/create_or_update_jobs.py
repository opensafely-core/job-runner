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

from opentelemetry import trace
from pipeline import RUN_ALL_COMMAND, load_pipeline

from common import config as common_config
from common.lib.git import GitFileNotFoundError, read_file_from_repo
from common.lib.github_validators import validate_repo_and_commit
from controller import tracing
from controller.actions import get_action_specification
from controller.lib.database import exists_where, insert, transaction, update_where
from controller.models import Job, State, StatusCode
from controller.permissions.utils import build_analysis_scope
from controller.queries import calculate_workspace_state
from controller.reusable_actions import (
    resolve_reusable_action_references,
)


log = logging.getLogger(__name__)
tracer = trace.get_tracer("create_or_update_jobs")


class RapCreateRequestError(Exception):
    pass


class StaleCodelistError(RapCreateRequestError):
    pass


class NothingToDoError(RapCreateRequestError):
    pass


def create_jobs(rap_create_request):
    with tracer.start_as_current_span(
        "create_jobs", attributes=rap_create_request.get_tracing_span_attributes()
    ):
        validate_rap_create_request(rap_create_request)
        project_file = get_project_file(rap_create_request)

        pipeline_config = load_pipeline(project_file)

        latest_jobs = get_latest_jobs_for_actions_in_project(
            rap_create_request.backend, rap_create_request.workspace, pipeline_config
        )

        new_jobs = get_new_jobs_to_run(rap_create_request, pipeline_config, latest_jobs)
        assert_new_jobs_created(rap_create_request, new_jobs, latest_jobs)

        resolve_reusable_action_references(new_jobs)

        # check for database actions in the new jobs, and raise an exception if
        # codelists are out of date
        assert_codelists_ok(rap_create_request, new_jobs)

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
        insert_into_database(new_jobs)

        return len(new_jobs)


def validate_rap_create_request(rap_create_request):
    if not rap_create_request.requested_actions:
        raise RapCreateRequestError("At least one action must be supplied")
    if not rap_create_request.workspace:
        raise RapCreateRequestError("Workspace name cannot be blank")
    if re.search(r"[^a-zA-Z0-9_\-]", rap_create_request.workspace):
        raise RapCreateRequestError(
            "Invalid workspace name (allowed are alphanumeric, dash and underscore)"
        )

    if rap_create_request.backend not in common_config.BACKENDS:
        raise RapCreateRequestError(
            f"Invalid backend '{rap_create_request.backend}', allowed are: "
            + ", ".join(common_config.BACKENDS)
        )

    if rap_create_request.database_name not in common_config.VALID_DATABASE_NAMES:
        raise RapCreateRequestError(
            f"Invalid database name '{rap_create_request.database_name}', allowed are: "
            + ", ".join(common_config.VALID_DATABASE_NAMES)
        )

    # As this check may involve talking to the remote git server we only do it at
    # the end once all other checks have passed
    validate_repo_and_commit(
        common_config.ALLOWED_GITHUB_ORGS,
        rap_create_request.repo_url,
        rap_create_request.commit,
        rap_create_request.branch,
    )


def get_project_file(rap_create_request):
    try:
        return read_file_from_repo(
            rap_create_request.repo_url, rap_create_request.commit, "project.yaml"
        )
    except GitFileNotFoundError:  # pragma: no cover
        raise RapCreateRequestError(
            f"No project.yaml file found in {rap_create_request.repo_url}"
        )


def get_latest_jobs_for_actions_in_project(backend, workspace, pipeline_config):
    pipeline_actions = {action for action in pipeline_config.all_actions}
    return [
        job
        for job in calculate_workspace_state(backend, workspace)
        if job.action in pipeline_actions
    ]


def get_new_jobs_to_run(rap_create_request, pipeline_config, current_jobs):
    """
    Returns a list of new jobs to run in response to the supplied RAP CreateRequest

    Args:
        rap_create_request: CreateRequest instance
        project: dict representing the parsed project file from the CreateRequest RAP
        current_jobs: list containing the most recent Job for each action in
            the workspace
    """
    # Build a dict mapping action names to job instances
    jobs_by_action = {job.action: job for job in current_jobs}
    # Add new jobs to it by recursing through the dependency tree
    for action in get_actions_to_run(rap_create_request, pipeline_config):
        recursively_build_jobs(
            jobs_by_action, rap_create_request, pipeline_config, action
        )

    # Pick out the new jobs we've added and return them
    current_job_ids = {job.id for job in current_jobs}
    return [job for job in jobs_by_action.values() if job.id not in current_job_ids]


def get_actions_to_run(rap_create_request, pipeline_config):
    # Handle the special `run_all` action
    if RUN_ALL_COMMAND in rap_create_request.requested_actions:
        return pipeline_config.all_actions
    else:
        return rap_create_request.requested_actions


def recursively_build_jobs(jobs_by_action, rap_create_request, pipeline_config, action):
    """
    Recursively populate the `jobs_by_action` dict with jobs

    Args:
        jobs_by_action: A dict mapping action ID strings to Job instances
        rap_create_request: An instance of CreateRequest representing the RAP.
        pipeline_config: A Pipeline instance representing the pipeline configuration.
        action: The string ID of the action to be added as a job.
    """
    existing_job = jobs_by_action.get(action)
    if existing_job and not job_should_be_rerun(rap_create_request, existing_job):
        return

    action_spec = get_action_specification(
        pipeline_config,
        action,
    )

    # Walk over the dependencies of this action, creating any necessary jobs,
    # and ensure that this job waits for its dependencies to finish before it
    # starts
    wait_for_job_ids = []
    for required_action in action_spec.needs:
        recursively_build_jobs(
            jobs_by_action, rap_create_request, pipeline_config, required_action
        )
        required_job = jobs_by_action[required_action]
        if required_job.state in [State.PENDING, State.RUNNING]:
            wait_for_job_ids.append(required_job.id)

    timestamp = time.time()

    analysis_scope = {}
    if action_spec.action.is_database_action:
        analysis_scope = build_analysis_scope(
            rap_create_request.analysis_scope,
            rap_create_request.project,
            rap_create_request.repo_url,
        )

    job = Job(
        rap_id=rap_create_request.id,
        state=State.PENDING,
        status_code=StatusCode.CREATED,
        # time in nanoseconds
        status_code_updated_at=int(timestamp * 1e9),
        status_message="Created",
        repo_url=rap_create_request.repo_url,
        commit=rap_create_request.commit,
        workspace=rap_create_request.workspace,
        database_name=rap_create_request.database_name,
        requires_db=action_spec.action.is_database_action,
        action=action,
        wait_for_job_ids=wait_for_job_ids,
        requires_outputs_from=action_spec.needs,
        run_command=action_spec.run,
        output_spec=action_spec.outputs,
        created_at=int(timestamp),
        updated_at=int(timestamp),
        backend=rap_create_request.backend,
        branch=rap_create_request.branch,
        user=rap_create_request.created_by,
        project=rap_create_request.project,
        orgs=rap_create_request.orgs,
        analysis_scope=analysis_scope,
    )
    tracing.initialise_job_trace(job)

    # Add it to the dictionary of scheduled jobs
    jobs_by_action[action] = job


def job_should_be_rerun(rap_create_request, job):
    """
    Do we need to run the action referenced by this job again?
    """
    # Already running or about to run so don't start a new one
    if job.state in [State.PENDING, State.RUNNING]:
        return False
    # Explicitly requested actions always get re-run
    if job.action in rap_create_request.requested_actions:
        return True
    # If it's not an explicilty requested action then it's a dependency, and if
    # we're forcing all dependencies to run then we need to run this one
    if rap_create_request.force_run_dependencies:
        return True

    # Otherwise if it succeeded last time there's no need to run again
    if job.state == State.SUCCEEDED:
        return False
    # If it failed last time, re-run it by default
    elif job.state == State.FAILED:
        return True
    else:  # pragma: no cover
        raise ValueError(f"Invalid state: {job}")


def assert_new_jobs_created(rap_create_request, new_jobs, current_jobs):
    if new_jobs:
        return

    # There are two legitimate reasons we can end up with no new jobs to run:

    # One is that the "run all" action was requested but everything has already run
    # successfully or is already running. We raise the special `NothingToDoError` which
    # is treated as a successful outcome because we've already done everything that was
    # requested.
    if RUN_ALL_COMMAND in rap_create_request.requested_actions:
        raise NothingToDoError("All actions have already completed successfully")

    # The other reason is that every requested action is already running or pending,
    # this is considered a user error.
    current_job_states = {job.action: job.state for job in current_jobs}
    requested_action_states = {
        current_job_states.get(action)
        for action in rap_create_request.requested_actions
    }
    if requested_action_states <= {State.PENDING, State.RUNNING}:
        raise NothingToDoError("All requested actions were already scheduled to run")

    # But if we get here then we've somehow failed to schedule new jobs despite the fact
    # that some of the actions we depend on have failed, which is a bug.
    raise Exception(
        f"Unexpected job states after scheduling: {current_job_states}"
    )  # pragma: no cover


def assert_codelists_ok(rap_create_request, new_jobs):
    if rap_create_request.codelists_ok:
        return True
    for job in new_jobs:
        # Codelists are out of date; fail the entire job request if any job
        # requires database access
        if job.requires_db:
            raise StaleCodelistError(
                f"Codelists are out of date (required by action {job.action})"
            )


def insert_into_database(jobs):
    with transaction():
        for job in jobs:
            insert(job)


def related_jobs_exist(rap_create_request):
    return exists_where(Job, rap_id=rap_create_request.id)


def set_cancelled_flag_for_actions(rap_id, actions):
    # It's important that we modify the Jobs in-place in the database rather than retrieving, updating and re-writing
    # them. If we did the latter then we would risk dirty writes if the run thread modified a Job while we were
    # working.
    update_where(
        Job,
        {
            "cancelled": True,
            "completed_at": int(time.time()),
        },
        rap_id=rap_id,
        action__in=actions,
    )
