import logging
import sys
import time

from opentelemetry import trace

from jobrunner import config
from jobrunner.agent import task_api, tracing
from jobrunner.executors import get_executor_api
from jobrunner.job_executor import ExecutorAPI, ExecutorState, JobDefinition, JobStatus
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.schema import TaskType


log = logging.getLogger(__name__)
tracer = trace.get_tracer("agent_loop")

EXECUTOR_RETRIES = {}


class RetriesExceeded(Exception):
    pass


class InvalidTransition(Exception):
    pass


class ExecutorError(Exception):
    pass


def main(exit_callback=lambda _: False):  # pragma: no cover
    log.info("jobrunner.agent.main loop started")
    api = get_executor_api()

    while True:
        with tracer.start_as_current_span(
            "AGENT_LOOP", attributes={"agent_loop": True}
        ):
            active_tasks = handle_tasks(api)

        if exit_callback(active_tasks):
            break

        time.sleep(config.JOB_LOOP_INTERVAL)


def handle_tasks(api: ExecutorAPI | None):
    active_tasks = task_api.get_active_tasks()

    handled_tasks = []

    for task in active_tasks:
        # `set_log_context` ensures that all log messages triggered anywhere
        # further down the stack will have `task` set on them
        with set_log_context(task=task):
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
        mark_task_as_error(
            task,
            error=str(exc),
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

    with tracer.start_as_current_span("LOOP_TASK") as span:
        tracing.set_task_span_metadata(span, task)
        try:
            match task.type:
                case TaskType.RUNJOB:
                    handle_run_job_task(task, api)
                case TaskType.CANCELJOB:
                    handle_cancel_job_task(task, api)
                case _:
                    assert False, f"Unknown task type {task.type}"
        except Exception as exc:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
            span.record_exception(exc)
            raise


def handle_cancel_job_task(task, api):
    # A CANCELJOB task just sends a job_id in its task definition; construct
    # a dummy JobDefinition to use with the executor API. job ID is all we
    # need to find out the current status and do the actions required to
    # cancel and clean up
    # TODO: finalize() writes job logs, and will be missing some expected information
    # if only job_id is passed (from job definition fields, it expects to write
    # job id, job_request_id, created_at, database_name and commit)
    job = JobDefinition.from_dict(task.definition)

    job_status = api.get_status(job)

    span = trace.get_current_span()
    span.set_attributes({"id": job.id, "initial_job_status": job_status.state.name})

    with set_log_context(job_definition=job):
        match job_status.state:
            case ExecutorState.UNKNOWN | ExecutorState.FINALIZED:
                # The job hasn't started or has already finished
                final_status = job_status
            case ExecutorState.PREPARED:
                # Nb. no need to actually run finalize() in this case.
                final_status = JobStatus(ExecutorState.FINALIZED)
            case ExecutorState.EXECUTING:
                new_status = api.terminate(job)
                update_controller(task, new_status)
                # call finalize to write the job logs
                final_status = api.finalize(job)
            case ExecutorState.EXECUTED | ExecutorState.ERROR:
                # job has finished or errored, call finalize to write the job logs
                final_status = api.finalize(job)
            case _:  # pragma: no cover
                raise InvalidTransition(
                    f"unexpected state of job {job.id}: {job_status.state}"
                )

        if job_status.state in [
            ExecutorState.EXECUTING,
            ExecutorState.EXECUTED,
            ExecutorState.FINALIZED,
            ExecutorState.ERROR,
        ]:
            api.cleanup(job)

        update_controller(task, final_status, complete=True)


def handle_run_job_task(task, api):
    """Handle an active task.

    This contains the main state machine logic for a task. For the most part,
    state transitions follow the same logic, which is abstracted. Some
    transitions require special logic, mainly the initial and final states, as
    well as various operational modes.
    """
    job = JobDefinition.from_dict(task.definition)
    with set_log_context(job_definition=job):
        # TODO: if job.allow_database_access, then we need to populate job.env with
        # various secrets, as per run.py:job_to_job_definition

        job_status = api.get_status(job)

        span = trace.get_current_span()
        tracing.set_job_span_metadata(
            span, job, initial_job_status=job_status.state.name
        )

        # TODO: Update get_status to detect an error.json and read it.
        # I think that JobStatus should probably grow .error and .result fields,
        # which get_status can populate. Then all the logic is self contained.
        match job_status.state:
            case (
                ExecutorState.ERROR | ExecutorState.EXECUTING | ExecutorState.FINALIZED
            ):
                # No action needed, just inform the controller we are in this stage
                update_controller(task, job_status)

            case ExecutorState.UNKNOWN:
                # a new job
                # prepare is synchronous, which means set our code to PREPARING
                # before calling  api.prepare(), and we expect it to be PREPARED
                # when finished
                update_controller(task, JobStatus(ExecutorState.PREPARING))
                new_status = api.prepare(job)
                update_controller(task, new_status)

            case ExecutorState.PREPARED:
                if job.allow_database_access:
                    inject_db_secrets(job)

                new_status = api.execute(job)
                update_controller(task, new_status)

            case ExecutorState.EXECUTED:
                # finalize is also synchronous
                update_controller(task, JobStatus(ExecutorState.FINALIZING))
                new_status = api.finalize(job)
                update_controller(task, new_status)

                # We are not finalized, which is our final state - we have finished!
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
            case _:
                raise InvalidTransition(
                    f"unexpected state of job {job.id}: {job_status.state}"
                )


def update_controller(
    task, status: JobStatus, results: dict = None, complete: bool = False
):
    """
    Wrap the update_controller call to set the final job status on the current
    span from the agent_loop trace
    Note that we set final_job_status twice when we call prepare and finalise, as
    we update the controller before (when status is PREPARING/FINALIZING) and
    after, (when status is PREPARED/FINALIZED); this is OK, because we'll still
    record the final status at the end of this loop.
    """
    span = trace.get_current_span()
    attributes = {
        "final_job_status": status.state.name,
        "complete": complete,
    }
    if results is not None:
        results_attrs = results.get("results")
        if results_attrs:
            attributes.update(**results_attrs)
        error = results.get("error")
        if error:
            attributes.update(error=error)

    span.set_attributes(attributes)
    task_api.update_controller(task, status.state.value, results, complete)


def mark_task_as_error(task, error):
    """
    Pass error information on to the controller and mark this task as complete
    """
    # TODO: persist error info
    update_controller(
        task, JobStatus(ExecutorState.ERROR), results={"error": error}, complete=True
    )


def inject_db_secrets(job):
    """Inject the configured db secrets into the job's environ."""
    assert job.allow_database_access
    if config.USING_DUMMY_DATA_BACKEND:
        return

    job.env["DATABASE_URL"] = config.DATABASE_URLS[job.database_name]
    if config.TEMP_DATABASE_NAME:
        job.env["TEMP_DATABASE_NAME"] = config.TEMP_DATABASE_NAME
    if config.PRESTO_TLS_KEY and config.PRESTO_TLS_CERT:
        job.env["PRESTO_TLS_CERT"] = config.PRESTO_TLS_CERT
        job.env["PRESTO_TLS_KEY"] = config.PRESTO_TLS_KEY
    if config.EMIS_ORGANISATION_HASH:
        job.env["EMIS_ORGANISATION_HASH"] = config.EMIS_ORGANISATION_HASH


if __name__ == "__main__":  # pragma: no cover
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
