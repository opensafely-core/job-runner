import datetime
import logging
import os
import sys
import time

from opentelemetry import trace

from jobrunner import config, tracing
from jobrunner.agent import task_api
from jobrunner.executors import get_executor_api
from jobrunner.job_executor import (
    ExecutorAPI,
    ExecutorState,
    JobDefinition,
    Privacy,
    Study,
)
from jobrunner.lib import ns_timestamp_to_datetime
from jobrunner.lib.database import find_where, select_values, update
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.models import Job, State, StatusCode
from jobrunner.queries import calculate_workspace_state, get_flag_value


log = logging.getLogger(__name__)
tracer = trace.get_tracer("loop")

EXECUTOR_RETRIES = {}


class RetriesExceeded(Exception):
    pass


class InvalidTransition(Exception):
    pass


class ExecutorError(Exception):
    pass


def main(exit_callback=lambda _: False):  # pragma: no cover
    log.info("jobrunner.run loop started")
    api = get_executor_api()

    while True:
        with tracer.start_as_current_span("LOOP", attributes={"loop": True}):
            active_tasks = handle_tasks(api)

        if exit_callback(active_tasks):
            break

        time.sleep(config.JOB_LOOP_INTERVAL)


def handle_tasks(api: ExecutorAPI | None):
    active_tasks = task_api.get_active_tasks()

    handled_tasks = []

    for task in active_tasks:
        # `set_log_context` ensures that all log messages triggered anywhere
        # further down the stack will have `job` set on them
        with set_log_context(task=task.id):
            handle_single_task(task, api)

        handled_tasks.append(task)

    return handled_tasks


# we do not control the transition from these states, the executor does
STABLE_STATES = [
    ExecutorState.PREPARING,
    ExecutorState.EXECUTING,
    ExecutorState.FINALIZING,
]


def handle_single_task(task, api):
    """The top level handler for a task.

    Mainly exists to wrap the task handling in an exception handler.
    """
    # we re-read the flags before considering each task, so make sure they apply
    # as soon as possible when set.
    mode = get_flag_value("mode")
    try:
        trace_handle_task(task, api, mode)
    except Exception as exc:
        # TODO: change this function to update controller and save error info somewhere
        mark_job_as_failed(
            task,
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


def trace_handle_task(task, api, mode):
    """Call handle task with tracing."""

    with tracer.start_as_current_span("LOOP_JOB") as span:
        # TODO: we'll need an agent/task version of set_span_metadata (and possibly
        # more of tracing.py) as the current one sets info about a Job, and will
        # probably still be used in some for by the controller
        # tracing.set_span_metadata(span, task)
        try:
            handle_run_job_task(task, api, mode)
        except Exception as exc:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
            span.record_exception(exc)
            raise


def handle_run_job_task(task, api, mode=None):
    """Handle an active task.

    This contains the main state machine logic for a task. For the most part,
    state transitions follow the same logic, which is abstracted. Some
    transitions require special logic, mainly the initial and final states, as
    well as supporting cancellation and various operational modes.
    """
    job = JobDefinition.from_dict(task.definition)

    # only consider these modes if we are not about to cancel the job
    if not job_definition.cancelled:
        if mode == "db-maintenance" and job_definition.allow_database_access:
            log.warning(f"DB maintenance mode active, killing db job {job.id}")
            # we ignore the JobStatus returned from these API calls, as this is not a hard error
            api.terminate(job_definition)
            api.cleanup(job_definition)

            return (
                StatusCode.WAITING_DB_MAINTENANCE,
                "Waiting for database to finish maintenance",
            )

    initial_status = api.get_status(job_definition)

    # cancelled is driven by user request, so is handled explicitly first
    if job_definition.cancelled:
        # if initial_status.state == ExecutorState.EXECUTED the job has already finished, so we
        # don't need to do anything here
        if initial_status.state == ExecutorState.EXECUTING:
            api.terminate(job_definition)  # synchronous operation
            new_status = api.get_status(job_definition)
            new_statuscode, _default_message = STATE_MAP[new_status.state]
            return new_statuscode, "Cancelled whilst executing"
        if initial_status.state == ExecutorState.PREPARED:
            # Nb. no need to actually run finalize() in this case
            return StatusCode.FINALIZED, "Cancelled whilst prepared"
        if initial_status.state == ExecutorState.UNKNOWN:
            return StatusCode.CANCELLED_BY_USER, "Cancelled by user"

    # handle the simple no change needed states.
    if initial_status.state in STABLE_STATES:
        # no action needed, simply update job message and timestamp, which is likely a no-op
        return STATE_MAP[initial_status.state]

    # TODO: We can never get here from get_status. Api methods should raise
    # ExecutorError instead of returning an error job status
    if initial_status.state == ExecutorState.ERROR:
        # something has gone wrong since we last checked
        raise ExecutorError(initial_status.message)

    # ok, handle the state transitions that are our responsibility
    if initial_status.state == ExecutorState.UNKNOWN:
        # a new job
        # prepare is synchronous, which means set our code to PREPARING
        # before calling  api.prepare(), and we expect it to be PREPARED
        # when finished
        # TODO: update controller before and after calling prepare
        new_status = api.prepare(job_definition)
        return STATE_MAP[new_status.state]

    elif initial_status.state == ExecutorState.PREPARED:
        new_status = api.execute(job_definition)
        return STATE_MAP[new_status.state]

    elif initial_status.state == ExecutorState.EXECUTED:
        # TODO: update controller before and after calling finalize
        new_status = api.finalize(job_definition)
        return STATE_MAP[new_status.state]

    elif initial_status.state == ExecutorState.FINALIZED:  # pragma: no branch
        # Cancelled jobs that have had cleanup() should now be again set to cancelled here to ensure
        # they finish in the FAILED state
        if job_definition.cancelled:
            # TODO: update controller
            # TODO: move cleanup to finalize?
            api.cleanup(job_definition)
            return

        # final state - we have finished!
        results = api.get_results(job_definition)
        # TODO: update controller with results

        api.cleanup(job_definition)

        # we are done here
        return

    # following logic is common to all non-final transitions

    else:
        raise InvalidTransition(
            f"unexpected state transition of job {job.id} from {initial_status.state} to {new_status.state}: {new_status.message}"
        )


# TODO: we will want to save error info in case the controller asks us about this job again
def mark_job_as_failed(job, code, message, error=None, **attrs):
    if error is None:
        error = True


def get_reason_job_not_started(job):
    log.debug("Querying for running jobs")
    running_jobs = find_where(Job, state=State.RUNNING)
    log.debug("Query done")
    used_resources = sum(
        get_job_resource_weight(running_job) for running_job in running_jobs
    )
    required_resources = get_job_resource_weight(job)
    if used_resources + required_resources > config.MAX_WORKERS:
        if required_resources > 1:  # pragma: no cover
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

    if os.environ.get("FUNTIMES", False):  # pragma: no cover
        # allow any db job to run
        if job.requires_db:
            return None

        # allow OSI non-db jobs to run
        if job.workspace.endswith("-interactive"):
            return None

        # nope all other jobs
        return StatusCode.WAITING_ON_WORKERS, "Waiting on available workers"


def list_outputs_from_action(workspace, action):
    for job in calculate_workspace_state(workspace):
        if job.action == action:
            return job.output_files

    # The action has never been run before
    return []


def get_job_resource_weight(job, weights=config.JOB_RESOURCE_WEIGHTS):
    """
    Get the job's resource weight by checking its workspace and action against
    the config file, default to 1 otherwise
    """
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


if __name__ == "__main__":  # pragma: no cover
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
