import logging
import sys
import time
import traceback

from opentelemetry import trace

from jobrunner.agent import task_api, tracing
from jobrunner.config import agent as config
from jobrunner.config import common as common_config
from jobrunner.executors import get_executor_api
from jobrunner.job_executor import ExecutorAPI, ExecutorState, JobDefinition, JobStatus
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.schema import TaskType


log = logging.getLogger(__name__)
tracer = trace.get_tracer("agent_loop")


class InvalidTransition(Exception):
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

        time.sleep(common_config.JOB_LOOP_INTERVAL)


def handle_tasks(api: ExecutorAPI | None):
    active_tasks = task_api.get_active_tasks(backend=config.BACKEND)

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

    Mainly exists to wrap the task handling in an exception handler, and trace it.
    """

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
            mark_task_as_error(
                api,
                task,
                sys.exc_info(),
            )
            # Do not clean up, as we may want to debug
            #
            # Raising will kill the main loop, by design. The service manager
            # will restart, and this job will be ignored when it does, as
            # it has failed. If we have an internal error, a full restart
            # might recover better.
            raise


def handle_cancel_job_task(task, api):
    """
    Handle cancelling a job. The actions required to terminate, finalize and clean
    up a job depend on its state at the point of cancellation
    """
    job = JobDefinition.from_dict(task.definition)

    initial_job_status = api.get_status(job, cancelled=True)

    span = trace.get_current_span()
    span.set_attributes(
        {"id": job.id, "initial_job_status": initial_job_status.state.name}
    )

    # initialize pre_finalized_job_status as initial_job_status
    # this may change during the cancellation process, depending on what we need to
    # do with the job. At the end of the process, we'll log the state change from
    # pre_finalized_job_status to final_status
    pre_finalized_job_status = initial_job_status

    # tell the controller what stage we're at now
    update_job_task(task, initial_job_status, previous_status=None)

    with set_log_context(job_definition=job):
        match initial_job_status.state:
            case ExecutorState.FINALIZED:
                # The job has already finished and been finalized, nothing to do here
                final_status = initial_job_status
            case (
                ExecutorState.UNKNOWN
                | ExecutorState.PREPARED
                | ExecutorState.EXECUTED
                | ExecutorState.ERROR
            ):
                # States wehere we need to run finalize()
                # If the job hasn't started; we run finalize() to record metadata, including
                # its cancelled state. If it's finished or errored, finalize() will also write the job logs
                final_status = api.finalize(job, cancelled=True)
            case ExecutorState.EXECUTING:
                pre_finalized_job_status = api.terminate(job)
                update_job_task(
                    task, pre_finalized_job_status, previous_status=initial_job_status
                )
                # call finalize to write the job logs
                final_status = api.finalize(job, cancelled=True)
            case _:  # pragma: no cover
                raise InvalidTransition(
                    f"unexpected state of job {job.id}: {initial_job_status.state}"
                )

        # Clean up based on the starting job state
        if initial_job_status.state in [
            ExecutorState.EXECUTING,
            ExecutorState.EXECUTED,
            ExecutorState.FINALIZED,
            ExecutorState.ERROR,
        ]:
            api.cleanup(job)

        update_job_task(
            task, final_status, previous_status=pre_finalized_job_status, complete=True
        )


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
            case ExecutorState.ERROR | ExecutorState.FINALIZED:
                # No action needed, just inform the controller we are in this completed stage
                update_job_task(
                    task, job_status, previous_status=job_status, complete=True
                )

            case ExecutorState.EXECUTING:
                # Still waitin'
                update_job_task(task, job_status, previous_status=job_status)

            case ExecutorState.UNKNOWN:
                # a new job
                # prepare is synchronous, which means set our code to PREPARING
                # before calling  api.prepare(), and we expect it to be PREPARED
                # when finished
                preparing_status = JobStatus(ExecutorState.PREPARING)
                update_job_task(task, preparing_status, previous_status=job_status)
                new_status = api.prepare(job)
                update_job_task(task, new_status, previous_status=preparing_status)

            case ExecutorState.PREPARED:
                if job.allow_database_access:
                    inject_db_secrets(job)

                new_status = api.execute(job)
                update_job_task(task, new_status, previous_status=job_status)

            case ExecutorState.EXECUTED:
                # finalize is also synchronous
                finalizing_status = JobStatus(ExecutorState.FINALIZING)
                update_job_task(task, finalizing_status, previous_status=job_status)
                new_status = api.finalize(job)
                api.cleanup(job)

                # We are now finalized, which is our final state - we have finished!
                # Cleanup and update controller with results
                update_job_task(
                    task,
                    new_status,
                    previous_status=finalizing_status,
                    complete=True,
                )
            case _:
                assert False, f"unexpected state of job {job.id}: {job_status.state}"


def update_job_task(
    task,
    status: JobStatus,
    previous_status: JobStatus = None,
    complete: bool = False,
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
    tracing.set_job_results_metadata(span, status.results, attributes)
    log_state_change(task, status, previous_status)
    task_api.update_controller(task, status.state.value, status.results, complete)


def log_state_change(task, status, previous_status):
    previous_state = previous_status.state if previous_status is not None else None
    if status.state == previous_state:
        return
    log_message = f"State change for job {task.definition['id']}: {previous_state} -> {status.state}"

    log.info(log_message)


def mark_task_as_error(api, task, exc_info):
    """
    Pass error information on to the controller and mark this task as complete
    """
    exc_type, exc, tb = exc_info
    error = {
        "exception": exc_type.__name__,
        "message": str(exc),
        "traceback": "".join(traceback.format_exception(exc_type, exc, tb)),
    }

    match task.type:
        case TaskType.RUNJOB | TaskType.CANCELJOB:
            job = JobDefinition.from_dict(task.definition)
            # this will persist the exception info to disk
            status = api.finalize(job, error=error)
            update_job_task(task, status, complete=True)
        case _:
            assert False


def inject_db_secrets(job):
    """Inject the configured db secrets into the job's environ."""
    assert job.allow_database_access
    if config.USING_DUMMY_DATA_BACKEND:
        return

    if job.database_name not in config.DATABASE_URLS:
        backend = job.env["OPENSAFELY_BACKEND"]
        raise ValueError(
            f"Database name '{job.database_name}' is not currently defined "
            f"for backend '{backend}'"
        )

    job.env["DATABASE_URL"] = config.DATABASE_URLS[job.database_name]
    if config.TEMP_DATABASE_NAME:
        job.env["TEMP_DATABASE_NAME"] = config.TEMP_DATABASE_NAME
    if config.PRESTO_TLS_KEY and config.PRESTO_TLS_CERT:
        job.env["PRESTO_TLS_CERT"] = config.PRESTO_TLS_CERT
        job.env["PRESTO_TLS_KEY"] = config.PRESTO_TLS_KEY
    if config.EMIS_ORGANISATION_HASH:
        job.env["EMIS_ORGANISATION_HASH"] = config.EMIS_ORGANISATION_HASH


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
