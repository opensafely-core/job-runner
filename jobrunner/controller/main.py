"""
Script which polls the database for active (i.e. non-terminated) jobs, takes
the appropriate action for each job depending on its current state, and then
updates its state as appropriate.
"""

import collections
import datetime
import logging
import sys
import time

from opentelemetry import trace

from jobrunner import tracing
from jobrunner.config import common as common_config
from jobrunner.config import controller as config
from jobrunner.controller.task_api import insert_task, mark_task_inactive
from jobrunner.job_executor import (
    JobDefinition,
    JobResults,
    Study,
)
from jobrunner.lib import ns_timestamp_to_datetime
from jobrunner.lib.database import find_where, select_values, transaction, update
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.models import Job, State, StatusCode, Task, TaskType
from jobrunner.queries import calculate_workspace_state, get_flag_value


log = logging.getLogger(__name__)
tracer = trace.get_tracer("loop")


class PlatformError(Exception):
    """
    Indicates that something went wrong when running a job that is external to anything
    that happened within the container
    """


def main(exit_callback=lambda _: False):
    log.info("jobrunner.run loop started")

    while True:
        with tracer.start_as_current_span("LOOP", attributes={"loop": True}):
            active_jobs = handle_jobs()

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
    # TODO: These flags are going to need to be set per-backend so we'll need to figure
    # out how to do that and then retrive the values for the backend associated with
    # each job
    mode = get_flag_value("mode")
    paused = str(get_flag_value("paused", "False")).lower() == "true"
    try:
        trace_handle_job(job, mode, paused)
    except Exception as exc:
        mark_job_as_failed(
            job,
            StatusCode.INTERNAL_ERROR,
            "Internal error: this usually means a platform issue rather than a problem "
            "for users to fix.\n"
            "The tech team are automatically notified of these errors and will be "
            "investigating.",
            error=exc,
        )
        # Do not clean up, as we may want to debug
        #
        # Raising will kill the main loop, by design. The service manager
        # will restart, and this job will be ignored when it does, as
        # it has failed. If we have an internal error, a full restart
        # might recover better.
        raise


def trace_handle_job(job, mode, paused):
    """Call handle job with tracing."""
    attrs = {
        "initial_state": job.state.name,
        "initial_code": job.status_code.name,
    }

    with tracer.start_as_current_span("LOOP_JOB") as span:
        tracing.set_span_metadata(span, job, **attrs)
        try:
            handle_job(job, mode, paused)
        except Exception as exc:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
            span.record_exception(exc)
            raise
        else:
            span.set_attribute("final_state", job.state.name)
            span.set_attribute("final_code", job.status_code.name)


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
            mark_job_as_failed(job, StatusCode.CANCELLED_BY_USER, "Cancelled by user")
        return

    # Handle special modes. TODO: These need to be made backend-specific
    if paused:
        if job.state == State.PENDING:
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

    # A new job, which may or may not be ready to start
    if job.state == State.PENDING:
        # Check states of jobs we're depending on
        awaited_states = get_states_of_awaited_jobs(job)
        if State.FAILED in awaited_states:
            mark_job_as_failed(
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
            set_code(job, StatusCode.EXECUTING, "Executing job on the backend")
    else:
        assert job.state == State.RUNNING
        task = get_task_for_job(job)
        assert task is not None
        if task.agent_complete:
            if job_error := task.agent_results.get("error"):
                # Handled elsewhere
                raise PlatformError(job_error)
            else:
                job_results = JobResults.from_dict(task.agent_results)
                save_results(job, job_results)
                # TODO: Delete obsolete files


def save_results(job, results):
    """Extract the results of the execution and update the job accordingly."""
    # save job outputs
    job.outputs = results.outputs
    job.level4_excluded_files = results.level4_excluded_files

    message = None
    error = False

    if results.exit_code != 0:
        code = StatusCode.NONZERO_EXIT
        error = True
        message = "Job exited with an error"
        if results.message:
            message += f": {results.message}"
        elif job.requires_db:
            error_msg = config.DATABASE_EXIT_CODES.get(results.exit_code)
            if error_msg:
                message += f": {error_msg}"

    elif results.unmatched_patterns:
        job.unmatched_outputs = results.unmatched_outputs
        code = StatusCode.UNMATCHED_PATTERNS
        error = True
        # If the job fails because an output was missing its very useful to
        # show the user what files were created as often the issue is just a
        # typo
        message = "No outputs found matching patterns:\n - {}".format(
            "\n - ".join(results.unmatched_patterns)
        )

    else:
        code = StatusCode.SUCCEEDED
        message = "Completed successfully"

        if results.level4_excluded_files:
            message += f", but {len(results.level4_excluded_files)} file(s) marked as moderately_sensitive were excluded. See job log for details."

    set_code(job, code, message, error=error, results=results)


def job_to_job_definition(job):
    env = {"OPENSAFELY_BACKEND": job.backend}

    # Prepend registry name
    action_args = job.action_args
    image = action_args.pop(0)
    full_image = f"{common_config.DOCKER_REGISTRY}/{image}"
    if image.startswith("stata-mp"):
        env["STATA_LICENSE"] = str(config.STATA_LICENSE)

    # Jobs which are running reusable actions pull their code from the reusable
    # action repo, all other jobs pull their code from the study repo
    study = Study(job.action_repo_url or job.repo_url, job.action_commit or job.commit)
    # Both of action commit and repo_url should be set if either are
    assert bool(job.action_commit) == bool(job.action_repo_url)

    input_files = []
    for action in job.requires_outputs_from:
        for filename in list_outputs_from_action(job.backend, job.workspace, action):
            input_files.append(filename)

    outputs = {}
    for privacy_level, named_patterns in job.output_spec.items():
        for name, pattern in named_patterns.items():
            outputs[pattern] = privacy_level

    return JobDefinition(
        id=job.id,
        job_request_id=job.job_request_id,
        study=study,
        workspace=job.workspace,
        action=job.action,
        created_at=job.created_at,
        image=full_image,
        args=action_args,
        env=env,
        inputs=input_files,
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


def mark_job_as_failed(job, code, message, error=None, **attrs):
    if error is None:
        error = True

    set_code(job, code, message, error=error, **attrs)


def set_code(job, new_status_code, message, error=None, results=None, **attrs):
    """Set the granular status code state.

    We also trace this transition with OpenTelemetry traces.

    Note: timestamp precision in the db is to the nearest second, which made
    sense when we were tracking fewer high level states. But now we are
    tracking more granular states, subsecond precision is needed to avoid odd
    collisions when states transition in <1s. Due to this, timestamp parameter
    should be the output of time.time() i.e. a float representing seconds.
    """
    t = time.time()
    timestamp_s = int(t)
    timestamp_ns = int(t * 1e9)

    # if status code has changed then trace it and update
    if job.status_code != new_status_code:
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
            StatusCode.PREPARED,
            StatusCode.PREPARING,
            StatusCode.EXECUTING,
        ]:
            # we've started running
            job.state = State.RUNNING
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
        elif new_status_code in [
            StatusCode.WAITING_ON_REBOOT,
            StatusCode.WAITING_DB_MAINTENANCE,
        ]:
            job.state = State.PENDING
            job.started_at = None

        # job trace: we finished the previous state
        tracing.finish_current_state(
            job, timestamp_ns, error=error, message=message, results=results, **attrs
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
            tracing.record_final_state(
                job,
                timestamp_ns,
                error=error,
                message=message,
                results=results,
                **attrs,
            )

        log.info(job.status_message, extra={"status_code": job.status_code})

    # If the status message hasn't changed then we only update the timestamp
    # once a minute. This gives the user some confidence that the job is still
    # active without writing to the database every single time we poll
    elif timestamp_s - job.updated_at >= 60:
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


def get_reason_job_not_started(job):
    log.debug("Querying for running jobs")
    running_jobs = find_where(Job, state=State.RUNNING, backend=job.backend)
    log.debug("Query done")
    used_resources = sum(
        get_job_resource_weight(running_job) for running_job in running_jobs
    )
    required_resources = get_job_resource_weight(job)
    if used_resources + required_resources > config.MAX_WORKERS:
        if required_resources > 1:
            return (
                StatusCode.WAITING_ON_WORKERS,
                "Waiting on available workers for resource intensive job",
            )
        else:
            return StatusCode.WAITING_ON_WORKERS, "Waiting on available workers"

    if job.requires_db:
        running_db_jobs = len([j for j in running_jobs if j.requires_db])
        if running_db_jobs >= config.MAX_DB_WORKERS:
            return (
                StatusCode.WAITING_ON_DB_WORKERS,
                "Waiting on available database workers",
            )


def list_outputs_from_action(backend, workspace, action):
    for job in calculate_workspace_state(backend, workspace):
        if job.action == action:
            return job.output_files

    # The action has never been run before
    return []


def get_job_resource_weight(job, weights=None):
    """
    Get the job's resource weight by checking its workspace and action against
    the config file, default to 1 otherwise
    """
    weights = weights or config.JOB_RESOURCE_WEIGHTS
    action_patterns = weights.get(job.workspace)
    if action_patterns:
        for pattern, weight in action_patterns.items():
            if pattern.fullmatch(job.action):
                return weight
    return 1


def update_job(job):
    # The cancelled field is written by the sync thread and we should never update it. The sync thread never updates
    # any other fields after it has created the job, so we're always safe to modify them.
    update(job, exclude_fields=["cancelled"])


def create_task_for_job(job):
    previous_tasks = find_where(
        Task, id__like=f"{job.id}-%", type=TaskType.RUNJOB, backend=job.backend
    )
    assert all(t.active is False for t in previous_tasks)
    task_number = len(previous_tasks) + 1
    return Task(
        # Zero-pad the task number so tasks sort lexically
        id=f"{job.id}-{task_number:03}",
        type=TaskType.RUNJOB,
        definition=job_to_job_definition(job).to_dict(),
        backend=job.backend,
    )


def get_task_for_job(job):
    # TODO: I think jobs need to store the ID of the task they are currently associated
    # with. But for now, it works to always get the most recently created task for a
    # given job.
    tasks = find_where(
        Task, id__like=f"{job.id}-%", type=TaskType.RUNJOB, backend=job.backend
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
    canceljob_task = Task(
        id=f"{runjob_task.id}-cancel",
        type=TaskType.CANCELJOB,
        definition=job_to_job_definition(job).to_dict(),
        # TODO: Uncomment this when Job grows a `backend` field
        backend="",  # job.backend,
    )
    insert_task(canceljob_task)


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
