import logging
import sys
import time
import traceback

from opentelemetry import trace

from agent import task_api, tracing
from agent.executors import get_executor_api
from jobrunner.config import agent as config
from jobrunner.config import common as common_config
from jobrunner.job_executor import ExecutorAPI, ExecutorState, JobDefinition, JobStatus
from jobrunner.lib.docker import docker, get_network_config_args
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.schema import TaskType


log = logging.getLogger(__name__)
tracer = trace.get_tracer("agent_loop")


def main(exit_callback=lambda _: False):  # pragma: no cover
    log.info("agent.main loop started")
    api = get_executor_api()

    while True:
        active_tasks = handle_tasks(api)

        if exit_callback(active_tasks):
            break

        time.sleep(common_config.JOB_LOOP_INTERVAL)


def handle_tasks(api: ExecutorAPI | None):
    active_tasks = task_api.get_active_tasks()

    handled_tasks = []
    errored_tasks = []

    with tracer.start_as_current_span("AGENT_LOOP") as span:
        for task in active_tasks:
            # `set_log_context` ensures that all log messages triggered anywhere
            # further down the stack will have `task` set on them
            with set_log_context(task=task):
                try:
                    handle_single_task(task, api)
                except Exception:
                    # do not raise now, but record and move on to the next task, so
                    # we do not block loop.
                    log.exception("task error")
                    errored_tasks.append(task)

            handled_tasks.append(task)

        span.set_attributes(
            {"handled_tasks": len(handled_tasks), "errored_tasks": len(errored_tasks)}
        )

    if errored_tasks:
        raise Exception("Some tasks failed, restarting agent loop")

    return handled_tasks


def handle_single_task(task, api):
    """The top level handler for a task.

    Calls the appropriate handle_*_job_task function for the new task, and
    wraps the task handling in an exception handler.
    Also creates telemetry span for this task.
    """

    with tracer.start_as_current_span("LOOP_TASK") as span:
        tracing.set_task_span_metadata(span, task)
        try:
            match task.type:
                case TaskType.RUNJOB:
                    handle_run_job_task(task, api)
                case TaskType.CANCELJOB:
                    handle_cancel_job_task(task, api)
                case TaskType.DBSTATUS:
                    handle_simple_task(db_status_task, task)
                case _:
                    assert False, f"Unknown task type {task.type}"
        except Exception as exc:
            if is_fatal_task_error(exc):
                span.set_attribute("fatal_task_error", True)
                mark_task_as_error(
                    api,
                    task,
                    sys.exc_info(),
                )
            else:
                span.set_attribute("fatal_task_error", False)
            # Do not clean up, as we may want to debug
            #
            # Raising will kill the main loop, by design. The service manager
            # will restart, and this task will be ignored when it does, as
            # it has failed. If we have an internal error, a full restart
            # might recover better.
            raise


def is_fatal_task_error(exc: Exception) -> bool:
    # To faciliate the migration to the split agent/controller world we don't currently
    # consider _any_ errors as hard failures. But we will do so later and we want to
    # ensure that these code paths are adequately tested so we provide a simple
    # mechanism to trigger these in tests.
    return "test_hard_failure" in str(exc)


def handle_cancel_job_task(task, api):
    """
    Handle cancelling a job. The actions required to terminate, finalize and clean
    up a job depend on its state at the point of cancellation.
    """
    job = JobDefinition.from_dict(task.definition)
    span = trace.get_current_span()
    tracing.set_job_span_metadata(span, job)

    initial_job_status = api.get_status(job, cancelled=True)
    span.set_attributes({"initial_job_status": initial_job_status.state.name})

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
                # Handle states where we need to run finalize()
                # If the job hasn't started; we run finalize() to record metadata, including
                # its cancelled state. If it's finished or errored, finalize() will also write the job logs
                api.finalize(job, cancelled=True)
                final_status = api.get_status(job)
            case ExecutorState.EXECUTING:
                api.terminate(job)
                pre_finalized_job_status = api.get_status(job)
                update_job_task(
                    task, pre_finalized_job_status, previous_status=initial_job_status
                )
                # call finalize to write the job logs
                api.finalize(job, cancelled=True)
                final_status = api.get_status(job)
            case _:
                assert False, (
                    f"unexpected state of job {job.id}: {initial_job_status.state}"
                )

        # Clean up containers and volumes
        # Note that if the job hasn't started (initial status UNKNOWN) or has
        # already finished (FINALIZED), there should be nothing to clean up, but
        # cleanup() will handle that
        api.cleanup(job)

        update_job_task(
            task, final_status, previous_status=pre_finalized_job_status, complete=True
        )


def handle_run_job_task(task, api):
    """Handle an active task.

    This contains the main state machine logic for a task. For the most part,
    state transitions follow the same logic, which is abstracted.
    """
    job = JobDefinition.from_dict(task.definition)
    span = trace.get_current_span()
    tracing.set_job_span_metadata(span, job)

    initial_job_status = api.get_status(job, cancelled=True)
    span.set_attributes({"initial_job_status": initial_job_status.state.name})

    with set_log_context(job_definition=job):
        job_status = api.get_status(job)

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
                api.prepare(job)
                new_status = api.get_status(job)
                update_job_task(task, new_status, previous_status=preparing_status)

            case ExecutorState.PREPARED:
                if job.allow_database_access:
                    inject_db_secrets(job)

                api.execute(job)
                new_status = api.get_status(job)
                update_job_task(task, new_status, previous_status=job_status)

            case ExecutorState.EXECUTED:
                # finalize is also synchronous
                finalizing_status = JobStatus(ExecutorState.FINALIZING)
                update_job_task(task, finalizing_status, previous_status=job_status)
                api.finalize(job)
                new_status = api.get_status(job)
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
    span from the agent_loop trace.
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
    redacted_results = redact_results(status.results)

    tracing.set_job_results_metadata(span, redacted_results, attributes)
    log_state_change(task, status, previous_status)
    task_api.update_controller(
        task=task,
        stage=status.state.value,
        results=redacted_results,
        complete=complete,
        timestamp_ns=status.timestamp_ns,
    )


def redact_results(results):
    """
    Redact output filenames and patterns from the results before
    they are sent to the controller.
    If there are unmatched outputs or patterns, we also redact the
    status_message and hint which may contain messages referring to
    the unmatched filenames or patterns.
    """
    if not results:
        return results
    results = {**results}
    results.pop("outputs", None)
    has_unmatched_outputs = bool(results.pop("unmatched_outputs", None))
    has_unmatched_patterns = bool(results.pop("unmatched_patterns", None))
    if has_unmatched_outputs or has_unmatched_patterns:
        results["status_message"] = ""
        results["hint"] = ""

    results.update(
        {
            "has_unmatched_patterns": has_unmatched_patterns,
            "has_level4_excluded_files": bool(
                results.pop("level4_excluded_files", None)
            ),
        }
    )
    return results


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
            api.finalize(job, error=error)
            status = api.get_status(job)
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


def handle_simple_task(task_function, task):
    """
    A "simple" task function is one which takes keyword arguments as supplied in the
    Task definition and returns a dictionary which will be reported under the `results`
    key of the Task results. Any exceptions are caught and reported under the `error`
    key of the Task results.
    """
    try:
        results = task_function(**task.definition)
    except Exception as exc:
        log.exception(f"Exception handling: {task}")
        task_results = {
            "results": None,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    else:
        task_results = {
            "results": results,
            "error": None,
        }
    task_api.update_controller(
        task,
        # `stage` is not relevant for simple tasks so we leave it blank
        stage="",
        results=task_results,
        complete=True,
    )


def db_status_task(*, database_name):
    log.info(f"Running DBSTATUS task on database {database_name!r}")
    database_url = config.DATABASE_URLS[database_name]
    # Restrict network access to just the database
    network_config_args = get_network_config_args(
        config.DATABASE_ACCESS_NETWORK, target_url=database_url
    )
    ps = docker(
        [
            "run",
            "--rm",
            "-e",
            "DATABASE_URL",
            *network_config_args,
            "ghcr.io/opensafely-core/tpp-database-utils",
            "in_maintenance_mode",
        ],
        env={"DATABASE_URL": database_url},
        check=True,
        capture_output=True,
        text=True,
    )
    last_line = ps.stdout.strip().split("\n")[-1].strip()
    # Restrict the status messages that can be returned so that even in the case of a
    # compromised status check container it's not possible to extract significant
    # quantities of data
    status_allowlist = {"", "db-maintenance"}
    if last_line not in status_allowlist:
        raise ValueError(
            f"Invalid status, expected one of: {','.join(status_allowlist)}"
        )
    span = trace.get_current_span()
    span.set_attribute("agent.db-maintenance", last_line == "db-maintenance")
    return {"status": last_line}


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
