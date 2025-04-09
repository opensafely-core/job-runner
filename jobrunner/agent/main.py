import logging
import sys
import time

from opentelemetry import trace

from jobrunner import config
from jobrunner.agent import task_api
from jobrunner.executors import get_executor_api
from jobrunner.job_executor import ExecutorAPI, ExecutorState, JobDefinition, JobStatus
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.models import StatusCode


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
def handle_single_task(task, api):
    """The top level handler for a task.

    Mainly exists to wrap the task handling in an exception handler.
    """
    # we re-read the flags before considering each task, so make sure they apply
    # as soon as possible when set.
    try:
        trace_handle_task(task, api)
    except Exception as exc:
        # TODO: change this function to update controller and save error info somewhere
        mark_task_as_error(
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


def trace_handle_task(task, api):
    """Call handle task with tracing."""

    with tracer.start_as_current_span("LOOP_JOB") as span:
        # TODO: we'll need an agent/task version of set_span_metadata (and possibly
        # more of tracing.py) as the current one sets info about a Job, and will
        # probably still be used in some for by the controller
        # tracing.set_span_metadata(span, task)
        try:
            handle_run_job_task(task, api)
        except Exception as exc:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
            span.record_exception(exc)
            raise


def handle_cancel_job_task(task, api):
    # TODO: make it work!
    # CODE DUMP - not working, just preserving all bits of cancellation logic
    # from handle_run_job_task for fixing up later
    job = JobDefinition.from_dict(task.definition)

    job_status = api.get_status(job)

    # TODO: check logic here
    # if job_status.state == ExecutorState.EXECUTED the job has already finished, so we
    # don't need to do anything here
    if job_status.state == ExecutorState.EXECUTING:
        api.terminate(job)  # synchronous operation
        new_status = api.get_status(job)  # Executed (if no error)
        update_controller(task, new_status.state, new_status.timestamp_ns)
        return
    if job_status.state == ExecutorState.PREPARED:
        # Nb. no need to actually run finalize() in this case. The FINALIZED
        # state will be handled and cleaned up further down the loop
        update_controller(task, ExecutorState.FINALIZED)
        return
    if job_status.state == ExecutorState.UNKNOWN:
        update_controller(task, ExecutorState.UNKNOWN)
        return

    # Cancelled jobs that have had cleanup() should now be again set to cancelled here to ensure
    # they finish in the FAILED state
    api.cleanup(job)
    update_controller(task, job_status.state, complete=True)


def handle_run_job_task(task, api):
    """Handle an active task.

    This contains the main state machine logic for a task. For the most part,
    state transitions follow the same logic, which is abstracted. Some
    transitions require special logic, mainly the initial and final states, as
    well as supporting cancellation and various operational modes.
    """
    job = JobDefinition.from_dict(task.definition)
    # TODO: if job.allow_database_access, then we need to populate job.env with
    # various secrets, as per run.py:job_to_job_definition

    job_status = api.get_status(job)

    # TODO: get current span and add these
    # attrs = {
    #     "initial_code": task.status_code.name,
    # }

    # TODO: Update get_status to detect an error.json and read it.
    # I think that JobStatus should probably grow .error and .result fields,
    # which get_status can populate. Then all the logic is self contained.
    if job_status.state == ExecutorState.ERROR:
        # something has gone wrong since we last checked
        # This is for idempotency of previous errors
        update_controller(task, job_status)

    # handle the simple no change needed states.
    if job_status.state == ExecutorState.EXECUTING:  # now only EXECUTING
        # no action needed, simply update job message and timestamp, which is likely a no-op
        update_controller(task, job_status)
        return

    # ok, handle the state transitions that are our responsibility
    elif job_status.state == ExecutorState.UNKNOWN:
        # a new job
        # prepare is synchronous, which means set our code to PREPARING
        # before calling  api.prepare(), and we expect it to be PREPARED
        # when finished
        update_controller(task, JobStatus(ExecutorState.PREPARING))
        new_status = api.prepare(job)
        update_controller(task, new_status)
        return

    elif job_status.state == ExecutorState.PREPARED:
        new_status = api.execute(job)
        update_controller(task, new_status)
        return

    elif job_status.state == ExecutorState.EXECUTED:
        # finalize is also synchronous
        update_controller(task, JobStatus(ExecutorState.FINALIZING))
        new_status = api.finalize(job)
        update_controller(task, new_status)

        # final state - we have finished!
        # we don't want JobResults
        results = api.get_metadata(job)
        # Cleanup and update controller with results
        api.cleanup(job)
        update_controller(
            task,
            new_status,
            {"results": results},
            complete=True,
        )
        return

    raise InvalidTransition(f"unexpected state of job {job.id}: {job_status.state}")


def update_controller(
    task, status: JobStatus, results: dict = None, complete: bool = False
):
    # TODO: wrap update_controller to get current span from loop trace and add these
    # span.set_attribute("final_code", job.status_code.name)
    # TODO: task trace telemetry
    # TODO: send any error or results
    task_api.update_controller(task, status.state.value, results, complete)


def mark_task_as_error(task, code, message, error=None, **attrs):
    if error is None:
        error = True

    # TODO: This used to call set_state; we will want to instead
    # update_controller and save error info somewhere in case the controller
    # asks us about this job again. We will need to update get_status to detect that this has happened and handle it in handle_task
    # Code is a StatusCode.INTERNAL_ERROR, which is not a TaskStage
    # update controller with ExecutorState.ERROR, and pass the error message somewhere?
    # update_controller(task, ExecutorState.ERROR)


if __name__ == "__main__":  # pragma: no cover
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
