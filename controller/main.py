"""
Script which polls the database for active (i.e. non-terminated) jobs, takes
the appropriate action for each job depending on its current state, and then
updates its state as appropriate.
"""

import collections
import datetime
import logging
import secrets
import sys
import time

from opentelemetry import trace

from common import config as common_config
from common.job_executor import (
    JobDefinition,
    Study,
)
from common.schema import JobTaskResults
from controller import config
from controller.models import Job, State, StatusCode, Task, TaskType
from controller.queries import (
    calculate_workspace_state,
    get_flag_value,
    get_saved_job_request,
)
from controller.task_api import insert_task, mark_task_inactive
from jobrunner import tracing
from jobrunner.lib import ns_timestamp_to_datetime
from jobrunner.lib.database import (
    exists_where,
    find_where,
    select_values,
    transaction,
    update,
    update_where,
)
from jobrunner.lib.log_utils import configure_logging, set_log_context


log = logging.getLogger(__name__)
tracer = trace.get_tracer("loop")


def main(exit_callback=lambda _: False):
    log.info("jobrunner.run loop started")

    while True:
        with tracer.start_as_current_span("LOOP", attributes={"loop": True}):
            active_jobs = handle_jobs()

        update_scheduled_tasks()

        if exit_callback(active_jobs):
            break

        time.sleep(common_config.JOB_LOOP_INTERVAL)


def handle_jobs():
    log.debug("Querying database for active jobs")
    active_jobs = find_where(Job, state__in=[State.PENDING, State.RUNNING])
    log.debug("Done query")

    running_for_workspace = collections.defaultdict(int)
    handled_jobs = []

    while active_jobs:
        # We need to re-sort on each loop because the number of running jobs per
        # workspace will change as we work our way through
        active_jobs.sort(
            key=lambda job: (
                # Process all running jobs first. Once we've processed all of these, the
                # counts in `running_for_workspace` will be up-to-date.
                0 if job.state == State.RUNNING else 1,
                # Then process PENDING jobs in order of how many are running in the
                # workspace for that backend. This gives a fairer allocation of capacity among
                # workspaces.
                running_for_workspace[(job.backend, job.workspace)],
                # DB jobs are more important than cpu jobs
                0 if job.requires_db else 1,
                # Finally use job age as a tie-breaker
                job.created_at,
            )
        )
        job = active_jobs.pop(0)

        # `set_log_context` ensures that all log messages triggered anywhere
        # further down the stack will have `job` set on them
        with set_log_context(job=job):
            handle_single_job(job)

        # Add running jobs to the backend/workspace count
        if job.state == State.RUNNING:
            running_for_workspace[(job.backend, job.workspace)] += 1

        handled_jobs.append(job)

    return handled_jobs


def handle_single_job(job):
    """The top level handler for a job.

    Mainly exists to wrap the job handling in an exception handler.
    """
    # we re-read the flags before considering each job, so make sure they apply
    # as soon as possible when set.
    mode = get_flag_value("mode", job.backend)
    paused = (
        str(get_flag_value("paused", job.backend, default="False")).lower() == "true"
    )
    attrs = {
        "job.initial_state": job.state.name,
        "job.initial_code": job.status_code.name,
    }

    with tracer.start_as_current_span("LOOP_JOB") as span:
        tracing.set_span_job_metadata(span, job, extra=attrs)
        try:
            handle_job(job, mode, paused)
        except Exception as exc:
            span.set_attribute("job.fatal_error", is_fatal_controller_error(exc))
            if is_fatal_controller_error(exc):
                set_code(
                    job,
                    StatusCode.INTERNAL_ERROR,
                    "Internal error: this usually means a platform issue rather than a problem "
                    "for users to fix.\n"
                    "The tech team are automatically notified of these errors and will be "
                    "investigating.",
                    exception=exc,
                )
            # Do not clean up, as we may want to debug
            #
            # Raising will kill the main loop, by design. The service manager
            # will restart, and this job will be ignored when it does, as
            # it has failed. If we have an internal error, a full restart
            # might recover better.
            raise
        else:
            span.set_attribute("job.final_state", job.state.name)
            span.set_attribute("job.final_code", job.status_code.name)


def is_fatal_controller_error(exc: Exception) -> bool:
    """Returns whether an Exception thrown by the controller should be fatal to the job."""
    # To faciliate the migration to the split agent/controller world we don't currently
    # consider _any_ errors as hard failures. But we will do so later and we want to
    # ensure that these code paths are adequately tested so we provide a simple
    # mechanism to trigger these in tests.
    return "test_hard_failure" in str(exc)


def is_fatal_job_error(exc: Exception) -> bool:
    """Returns whether an Exception thrown in the agent & returned to the controller should be fatal to the job."""
    # An example might be: if there is version skew between the agent & the controller
    # and the agent reports an exception due to an API change.
    return "test_job_failure" in str(exc)


def handle_job(job, mode=None, paused=None):
    """Handle an active job.

    This contains the main state machine logic for a job. For the most part,
    state transitions follow the same logic, which is abstracted. Some
    transitions require special logic, mainly the initial and final states, as
    well as supporting cancellation and various operational modes.
    """
    assert job.state in (State.PENDING, State.RUNNING)

    # Cancellation is driven by user request, so is handled explicitly first
    if job.cancelled:
        with transaction():
            cancel_job(job)
            set_code(job, StatusCode.CANCELLED_BY_USER, "Cancelled by user")
        return

    # Handle special modes
    if paused:
        if job.state == State.PENDING:
            if job.status_code == StatusCode.WAITING_ON_REBOOT:
                # This job was already reset in prepration for reboot, just
                # update that we've seen it
                refresh_job_timestamps(job)
            else:
                # Do not start the job, keep it pending
                set_code(
                    job,
                    StatusCode.WAITING_PAUSED,
                    "Backend is currently paused for maintenance, job will start once this is completed",
                )
            return

    if mode == "db-maintenance" and job.requires_db:
        with transaction():
            if job.state == State.RUNNING:
                log.warning(f"DB maintenance mode active, killing db job {job.id}")
                cancel_job(job)

            # Reset state to pending and exit
            set_code(
                job,
                StatusCode.WAITING_DB_MAINTENANCE,
                "Waiting for database to finish maintenance",
            )
        return

    match job.state:
        case State.PENDING:
            handle_pending_job(job)
        case State.RUNNING:
            handle_running_job(job)
        case _:
            assert False, f"unexpected job state {job.state}"


def handle_pending_job(job):
    assert job.state == State.PENDING

    # Check states of jobs we're depending on
    awaited_states = get_states_of_awaited_jobs(job)
    if State.FAILED in awaited_states:
        set_code(
            job,
            StatusCode.DEPENDENCY_FAILED,
            "Not starting as dependency failed",
        )
        return

    if any(state != State.SUCCEEDED for state in awaited_states):
        set_code(
            job,
            StatusCode.WAITING_ON_DEPENDENCIES,
            "Waiting on dependencies",
        )
        return

    # Give me ONE GOOD REASON why I shouldn't start you running right now!
    not_started_reason = get_reason_job_not_started(job)
    if not_started_reason:
        code, message = not_started_reason
        set_code(job, code, message)
        return

    task = create_task_for_job(job)
    with transaction():
        insert_task(task)
        set_code(job, StatusCode.INITIATED, "Job executing on the backend")
    return


def handle_running_job(job):
    assert job.state == State.RUNNING

    task = get_task_for_job(job)
    assert task is not None
    if task.agent_complete:
        if job_error := task.agent_results["error"]:
            span = trace.get_current_span()
            span.set_attribute("fatal_job_error", is_fatal_job_error(job_error))
            if is_fatal_job_error(job_error):
                set_code(
                    job,
                    StatusCode.JOB_ERROR,
                    "This job returned a fatal error.",
                    exception=job_error,
                )
            else:
                # mark task as waiting on new task, this will trigger the loop to respawn it
                set_code(
                    job,
                    StatusCode.WAITING_ON_NEW_TASK,
                    "This job returned an error that could be retriedwith a new task.",
                )

        else:
            results = JobTaskResults.from_dict(task.agent_results)
            save_results(job, results, task.agent_timestamp_ns)
            # TODO: Delete obsolete files
    else:
        # A task exists for this job already and it hasn't completed yet
        # The current task stage may be None if the agent hasn't sent back any
        # update yet, otherwise it should be in one of the running status codes which
        # mirror ExecutorState
        # (PREPARING, PREPARED, EXECUTING, EXECUTED, FINALIZING)
        # Note we won't get here with FINALIZED status, because at that stage it
        # will also be complete
        # In case a running job is updated with an unknown agent_stage (i.e. an
        # ExecutorState that is not a valid StatusCode (i.e. error, unknown), we
        # use the current job status_code as a default)
        set_code(
            job,
            StatusCode.from_value(task.agent_stage, default=job.status_code),
            job.status_message,
            task_timestamp_ns=task.agent_timestamp_ns,
        )


def save_results(job, results, timestamp_ns):
    """Extract the results of the execution and update the job accordingly."""
    message = None

    if results.exit_code != 0:
        code = StatusCode.NONZERO_EXIT
        message = "Job exited with an error"
        if results.message:
            message += f": {results.message}"
        elif job.requires_db:
            error_msg = config.DATABASE_EXIT_CODES.get(results.exit_code)
            if error_msg:
                message += f": {error_msg}"

    elif results.has_unmatched_patterns:
        code = StatusCode.UNMATCHED_PATTERNS
        # If the job fails because an output was missing its very useful to
        # inform the user as often the issue is just a typo
        message = "Outputs matching expected patterns were not found. See job log for details."

    else:
        code = StatusCode.SUCCEEDED
        message = "Completed successfully"

        if results.has_level4_excluded_files:
            message += ", but some file(s) marked as moderately_sensitive were excluded. See job log for details."

    set_code(job, code, message, results=results, task_timestamp_ns=timestamp_ns)


def job_to_job_definition(job, task_id):
    env = {"OPENSAFELY_BACKEND": job.backend}

    # Prepend registry name
    action_args = job.action_args
    image = action_args.pop(0)
    full_image = f"{common_config.DOCKER_REGISTRY}/{image}"

    if image.startswith("stata-mp"):
        env["STATA_LICENSE"] = str(config.STATA_LICENSE)

    if job.requires_db and job.repo_url in config.REPOS_WITH_EHRQL_EVENT_LEVEL_ACCESS:
        env["EHRQL_ENABLE_EVENT_LEVEL_QUERIES"] = "True"

    # Jobs which are running reusable actions pull their code from the reusable
    # action repo, all other jobs pull their code from the study repo
    study = Study(job.action_repo_url or job.repo_url, job.action_commit or job.commit)
    # Both of action commit and repo_url should be set if either are
    assert bool(job.action_commit) == bool(job.action_repo_url)

    input_job_ids = []
    for action in job.requires_outputs_from:
        if previous_job_id := job_id_from_action(job.backend, job.workspace, action):
            input_job_ids.append(previous_job_id)

    outputs = {}
    for privacy_level, named_patterns in job.output_spec.items():
        for name, pattern in named_patterns.items():
            outputs[pattern] = privacy_level

    return JobDefinition(
        id=job.id,
        job_request_id=job.job_request_id,
        task_id=task_id,
        study=study,
        workspace=job.workspace,
        action=job.action,
        created_at=job.created_at,
        image=full_image,
        args=action_args,
        env=env,
        inputs=[],
        input_job_ids=input_job_ids,
        output_spec=outputs,
        allow_database_access=job.requires_db,
        database_name=job.database_name if job.requires_db else None,
        # in future, these may come from the JobRequest, but for now, we have
        # config defaults.
        cpu_count=config.DEFAULT_JOB_CPU_COUNT[job.backend],
        memory_limit=config.DEFAULT_JOB_MEMORY_LIMIT[job.backend],
        level4_max_filesize=config.LEVEL4_MAX_FILESIZE,
        level4_max_csv_rows=config.LEVEL4_MAX_CSV_ROWS,
        level4_file_types=list(config.LEVEL4_FILE_TYPES),
    )


def get_states_of_awaited_jobs(job):
    job_ids = job.wait_for_job_ids
    if not job_ids:
        return []

    log.debug("Querying database for state of dependencies")
    states = select_values(Job, "state", id__in=job_ids)
    log.debug("Done query")
    return states


def set_code(
    job,
    new_status_code,
    message,
    *,
    exception: Exception | str | None = None,
    results=None,
    task_timestamp_ns=None,
):
    """Set the granular status code state.

    We also trace this transition with OpenTelemetry traces.

    Note: timestamp precision in the db is to the nearest second, which made
    sense when we were tracking fewer high level states. But now we are
    tracking more granular states, subsecond precision is needed to avoid odd
    collisions when states transition in <1s. Due to this, timestamp parameter
    should be the output of time.time() i.e. a float representing seconds.
    """
    current_timestamp_ns = int(time.time() * 1e9)
    if task_timestamp_ns is None:
        task_timestamp_ns = current_timestamp_ns

    # if status code has changed then trace it and update
    if job.status_code != new_status_code:
        # For a status change, we use the timestamp from the task. This records the more
        # accurate timestamp of the status change in the agent
        timestamp_ns = task_timestamp_ns
        timestamp_s = int(task_timestamp_ns / 1e9)
        # handle timer measurement errors
        if job.status_code_updated_at > timestamp_ns:
            # we somehow have a negative duration, which honeycomb does funny things with.
            # This can happen in tests, where things are fast, but we've seen it in production too.
            duration = datetime.timedelta(
                microseconds=int((timestamp_ns - job.status_code_updated_at) / 1e3)
            )
            log.warning(
                f"negative state duration of {duration}, clamping to 1ms\n"
                f"before: {job.status_code:<24} at {ns_timestamp_to_datetime(job.status_code_updated_at)}\n"
                f"after : {new_status_code:<24} at {ns_timestamp_to_datetime(timestamp_ns)}\n"
            )
            timestamp_ns = int(job.status_code_updated_at + 1e6)  # set duration to 1ms
            timestamp_s = int(timestamp_ns // 1e9)

        # update coarse state and timings for user
        if new_status_code in [
            StatusCode.INITIATED,
            StatusCode.PREPARED,
            StatusCode.PREPARING,
            StatusCode.EXECUTING,
        ]:
            # we've started running
            job.state = State.RUNNING
            # Only update started_at if it hasn't already been set
            if not job.started_at:
                job.started_at = timestamp_s
        elif new_status_code in [StatusCode.CANCELLED_BY_USER]:
            # only set this cancelled status after any finalize/cleanup processes
            job.state = State.FAILED
        elif new_status_code.is_final_code:
            job.completed_at = timestamp_s
            if new_status_code == StatusCode.SUCCEEDED:
                job.state = State.SUCCEEDED
            else:
                job.state = State.FAILED
        # we sometimes reset the job back to pending
        elif new_status_code.is_reset_code:
            job.state = State.PENDING
            job.started_at = None

        # job trace: we finished the previous state
        tracing.finish_current_job_state(
            job,
            timestamp_ns,
            exception=exception,
            results=results,
        )

        # update db object
        job.status_code = new_status_code
        job.status_message = message
        job.updated_at = timestamp_s

        # use higher precision timestamp for state change time
        job.status_code_updated_at = timestamp_ns
        update_job(job)

        if new_status_code.is_final_code:
            # transitioning to a final state, so just record that state
            tracing.record_final_job_state(
                job,
                timestamp_ns,
                exception=exception,
                results=results,
            )

        log.info(job.status_message, extra={"status_code": job.status_code})

    # If the status code hasn't changed then we only update the timestamp
    # once a minute. This gives the user some confidence that the job is still
    # active without writing to the database every single time we poll
    else:
        # For unchanged status codes, we use the current time as the timestamp
        timestamp_ns = current_timestamp_ns
        timestamp_s = int(current_timestamp_ns / 1e9)
        if timestamp_s - job.updated_at < 60:
            return
        job.updated_at = timestamp_s
        log.debug("Updating job timestamp")
        update_job(job)
        log.debug("Update done")
        # For long running jobs we don't want to fill the logs up with "Job X
        # is still running" messages, but it is useful to have semi-regular
        # confirmations in the logs that it is still running. The below will
        # log approximately once every 10 minutes.
        if datetime.datetime.fromtimestamp(timestamp_s).minute % 10 == 0:
            log.info(job.status_message, extra={"status_code": job.status_code})


def refresh_job_timestamps(job):
    # `set_code()` already contains logic to handle updating timestamps at an
    # appropriate frequency even if nothing about the job has changed, so we re-use that
    # logic here
    set_code(job, job.status_code, job.status_message)


def get_reason_job_not_started(job):
    log.debug("Querying for running jobs")
    running_jobs = find_where(Job, state=State.RUNNING, backend=job.backend)
    log.debug("Query done")
    used_resources = sum(
        get_job_resource_weight(running_job) for running_job in running_jobs
    )
    required_resources = get_job_resource_weight(job)
    if used_resources + required_resources > config.MAX_WORKERS[job.backend]:
        if required_resources > 1:
            return (
                StatusCode.WAITING_ON_WORKERS,
                "Waiting on available workers for resource intensive job",
            )
        else:
            return StatusCode.WAITING_ON_WORKERS, "Waiting on available workers"

    if job.requires_db:
        running_db_jobs = len([j for j in running_jobs if j.requires_db])
        if running_db_jobs >= config.MAX_DB_WORKERS[job.backend]:
            return (
                StatusCode.WAITING_ON_DB_WORKERS,
                "Waiting on available database workers",
            )


def job_id_from_action(backend, workspace, action):
    for job in calculate_workspace_state(backend, workspace):
        if job.action == action:
            return job.id

    # The action has never been run before
    return None


def get_job_resource_weight(job, weights=None):
    """
    Get the job's resource weight by checking its workspace and action against
    the config file, default to 1 otherwise
    """
    weights = weights or config.JOB_RESOURCE_WEIGHTS
    action_patterns = weights.get(job.backend, {}).get(job.workspace)
    if action_patterns:
        for pattern, weight in action_patterns.items():
            if pattern.fullmatch(job.action):
                return weight
    return 1


def update_job(job):
    # The cancelled field is written by the sync thread and we should never update it. The sync thread never updates
    # any other fields after it has created the job, so we're always safe to modify them.
    update(job, exclude_fields=["cancelled"])


def get_attributes_for_job_task(job):
    job_request = get_saved_job_request(job)

    return {
        "user": job_request.get("created_by", "unknown"),
        "project": job_request.get("project", "unknown"),
        "orgs": ",".join(job_request.get("orgs", [])),
    }


def create_task_for_job(job):
    previous_tasks = find_where(
        Task, id__glob=f"{job.id}-*", type=TaskType.RUNJOB, backend=job.backend
    )

    assert all(not t.active for t in previous_tasks)
    task_number = len(previous_tasks) + 1
    # Zero-pad the task number so tasks sort lexically
    task_id = f"{job.id}-{task_number:03}"
    return Task(
        # Zero-pad the task number so tasks sort lexically
        id=task_id,
        type=TaskType.RUNJOB,
        definition=job_to_job_definition(job, task_id).to_dict(),
        backend=job.backend,
        attributes=get_attributes_for_job_task(job),
    )


def get_task_for_job(job):
    # TODO: I think jobs need to store the ID of the task they are currently associated
    # with. But for now, it works to always get the most recently created task for a
    # given job.
    tasks = find_where(
        Task, id__glob=f"{job.id}-*", type=TaskType.RUNJOB, backend=job.backend
    )
    # Task IDs are constructed such that, for a given job, lexical order matches
    # creation order
    tasks.sort(key=lambda t: t.id)
    if tasks:
        assert all(not t.active for t in tasks[:-1])
        return tasks[-1]
    else:
        return None


def cancel_job(job):
    runjob_task = get_task_for_job(job)
    if not runjob_task or not runjob_task.active:
        return
    mark_task_inactive(runjob_task)
    task_id = f"{runjob_task.id}-cancel"
    canceljob_task = Task(
        id=task_id,
        type=TaskType.CANCELJOB,
        definition=job_to_job_definition(job, task_id).to_dict(),
        backend=job.backend,
        attributes=get_attributes_for_job_task(job),
    )
    insert_task(canceljob_task)


def update_scheduled_tasks():
    # This is the only scheduled task we currently have
    update_scheduled_task_for_db_maintenance()


def update_scheduled_task_for_db_maintenance():
    for backend in config.MAINTENANCE_ENABLED_BACKENDS:
        update_scheduled_task_for_db_maintenance_for_backend(backend)


def update_scheduled_task_for_db_maintenance_for_backend(backend):
    # If we're in manual maintenance mode then deactivate any running status check tasks
    # and exit
    if get_flag_value("manual-db-maintenance", backend):
        update_where(
            Task,
            {"active": False},
            type=TaskType.DBSTATUS,
            active=True,
            backend=backend,
        )
        return

    # If there's already an active task then there's nothing to do
    if exists_where(
        Task,
        type=TaskType.DBSTATUS,
        backend=backend,
        active=True,
    ):
        return

    # If there's a task that was completed within POLL_INTERVAL seconds of now then
    # there's nothing to do
    cutoff_time = int(time.time() - config.MAINTENANCE_POLL_INTERVAL)
    if exists_where(
        Task,
        type=TaskType.DBSTATUS,
        backend=backend,
        active=False,
        finished_at__gt=cutoff_time,
    ):
        return

    # Otherwise, create a new task
    insert_task(
        Task(
            # Add a bit of structure to the ID: this isn't strictly necessary – truly
            # random IDs should work just fine – but it may help with future debugging
            id=f"dbstatus-{datetime.date.today()}-{secrets.token_hex(10)}",
            type=TaskType.DBSTATUS,
            backend=backend,
            definition={"database_name": "default"},
        )
    )


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
