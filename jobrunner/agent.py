"""
Script which polls the database for active (i.e. non-terminated) jobs, takes
the appropriate action for each job depending on its current state, and then
updates its state as appropriate.
"""

import logging
import sys
import threading
import time
from typing import Optional

from opentelemetry import trace

from jobrunner import config, tracing
from jobrunner.controller import list_outputs_from_action, mark_job_as_failed, set_code
from jobrunner.executors import get_executor_api
from jobrunner.job_executor import (
    ExecutorAPI,
    ExecutorRetry,
    ExecutorState,
    JobDefinition,
    Study,
)
from jobrunner.lib.database import find_where
from jobrunner.lib.log_utils import configure_logging, set_log_context
from jobrunner.models import Job, State, StatusCode
from jobrunner.queries import get_flag_value


log = logging.getLogger(__name__)
tracer = trace.get_tracer("loop")

EXECUTOR_RETRIES = {}


class RetriesExceeded(Exception):
    pass


class InvalidTransition(Exception):
    pass


class ExecutorError(Exception):
    pass


def agent(exit_callback=lambda _: False):
    log.info("jobrunner.agent loop started")
    api = get_executor_api()

    while True:
        with tracer.start_as_current_span("LOOP", attributes={"loop": True}):
            handle_running_jobs(api)

        time.sleep(config.JOB_LOOP_INTERVAL)


def handle_running_jobs(api: Optional[ExecutorAPI]):

    # API GET /jobs
    running_jobs = [
        j
        for j in find_where(Job, state=State.RUNNING)
        if j.status_code != StatusCode.FINALIZED
    ]

    handled_jobs = []

    for job in running_jobs:
        # `set_log_context` ensures that all log messages triggered anywhere
        # further down the stack will have `job` set on them
        with set_log_context(job=job):
            job_definition = job_to_job_definition(job)

            try:
                mode = get_flag_value("mode")
                code, message = handle_running_job(job_definition, api, mode)
            except Exception as exc:
                # API POST /job/{id}/
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
            else:
                # API "POST" /job/{id}, current state
                set_code(job, code, message)

        handled_jobs.append(job)

    return handled_jobs


# we do not control the transition from these states, the executor does
STABLE_STATES = [
    ExecutorState.PREPARING,
    ExecutorState.EXECUTING,
    ExecutorState.FINALIZING,
]

# map ExecutorState to StatusCode
STATE_MAP = {
    ExecutorState.PREPARING: (
        StatusCode.PREPARING,
        "Preparing your code and workspace files",
    ),
    ExecutorState.PREPARED: (
        StatusCode.PREPARED,
        "Prepared and ready to run",
    ),
    ExecutorState.EXECUTING: (
        StatusCode.EXECUTING,
        "Executing job on the backend",
    ),
    ExecutorState.EXECUTED: (
        StatusCode.EXECUTED,
        "Job has finished executing and is waiting to be finalized",
    ),
    ExecutorState.FINALIZING: (
        StatusCode.FINALIZING,
        "Recording job results",
    ),
    ExecutorState.FINALIZED: (
        StatusCode.FINALIZED,
        "Finished recording results",
    ),
}


def handle_running_job(job_definition, api, mode=None):
    """Handle an running job."""

    try:
        initial_status = api.get_status(job_definition)
    except ExecutorRetry as retry:
        job_retries = EXECUTOR_RETRIES.get(job_definition.id, 0) + 1
        EXECUTOR_RETRIES[job_definition.id] = job_retries
        span = trace.get_current_span()
        span.set_attribute("executor_retry", True)
        span.set_attribute("executor_retry_message", str(retry))
        span.set_attribute("executor_retry_count", job_retries)
        log.info(f"ExecutorRetry: {retry}")
        return
    else:
        EXECUTOR_RETRIES.pop(job_definition.id, None)

    # cancelled is driven by user request, so is handled explicitly first
    if job_definition.cancelled:
        # if initial_status.state == ExecutorState.EXECUTED the job has already finished, so we
        # don't need to do anything here
        if initial_status.state == ExecutorState.EXECUTING:
            api.terminate(job_definition)  # synchronous operation
            new_status = api.get_status(job_definition)
            new_statuscode, _default_message = STATE_MAP[new_status.state]
            return new_statuscode, "Cancelled whilst executing"
        # FINALIZING and PREPARED should never be hit as they are synchronus
        if initial_status.state == ExecutorState.PREPARED:
            # Nb. no need to actually run finalize() in this case
            return (StatusCode.FINALIZED, "Cancelled whilst prepared")

    # only consider db maintenance if not already cancelled
    elif mode == "db-maintenance" and job_definition.allow_database_access:
        log.warning(f"DB maintenance mode active, killing db job {job_definition.id}")
        # we ignore the JobStatus returned from these API calls, as this is not a hard error
        api.terminate(job_definition)
        api.cleanup(job_definition)

        return (
            StatusCode.WAITING_DB_MAINTENANCE,
            "Waiting for database to finish maintenance",
        )

    # handle the simple no change needed states.
    if initial_status.state in STABLE_STATES:
        # no action needed, simply update job message and timestamp, which is likely a no-op
        return STATE_MAP[initial_status.state]

    if initial_status.state == ExecutorState.ERROR:
        # something has gone wrong since we last checked
        raise ExecutorError(initial_status.message)

    # ok, handle the state transitions that are our responsibility
    if initial_status.state == ExecutorState.UNKNOWN:
        # prepare is synchronous, which means set our code to PREPARING
        # before calling api.prepare(), and we expect it to be PREPARED
        # when finished
        expected_state = ExecutorState.PREPARED
        new_status = api.prepare(job_definition)

    elif initial_status.state == ExecutorState.PREPARED:
        expected_state = ExecutorState.EXECUTING
        new_status = api.execute(job_definition)

    elif initial_status.state == ExecutorState.EXECUTED:
        # finalize is synchronous, which means set our code to FINALIZING
        # before calling  api.finalize(), and we expect it to be FINALIZED
        # when finished
        expected_state = ExecutorState.FINALIZED
        new_status = api.finalize(job_definition)

    if new_status.state == expected_state:
        # successful state change to the expected next state
        code, message = STATE_MAP[new_status.state]
        return (code, message)

    elif new_status.state == ExecutorState.ERROR:
        # all transitions can go straight to error
        raise ExecutorError(new_status.message)

    else:
        raise InvalidTransition(
            f"unexpected state transition of job {job_definition.id} from {initial_status.state} to {new_status.state}: {new_status.message}"
        )


def job_to_job_definition(job):
    allow_database_access = False
    env = {"OPENSAFELY_BACKEND": config.BACKEND}
    if job.requires_db:
        if not config.USING_DUMMY_DATA_BACKEND:
            allow_database_access = True
            env["DATABASE_URL"] = config.DATABASE_URLS[job.database_name]
            if config.TEMP_DATABASE_NAME:
                env["TEMP_DATABASE_NAME"] = config.TEMP_DATABASE_NAME
            if config.PRESTO_TLS_KEY and config.PRESTO_TLS_CERT:
                env["PRESTO_TLS_CERT"] = config.PRESTO_TLS_CERT
                env["PRESTO_TLS_KEY"] = config.PRESTO_TLS_KEY
            if config.EMIS_ORGANISATION_HASH:
                env["EMIS_ORGANISATION_HASH"] = config.EMIS_ORGANISATION_HASH
    # Prepend registry name
    action_args = job.action_args
    image = action_args.pop(0)
    full_image = f"{config.DOCKER_REGISTRY}/{image}"
    if image.startswith("stata-mp"):
        env["STATA_LICENSE"] = str(config.STATA_LICENSE)

    # Jobs which are running reusable actions pull their code from the reusable
    # action repo, all other jobs pull their code from the study repo
    study = Study(job.action_repo_url or job.repo_url, job.action_commit or job.commit)
    # Both of action commit and repo_url should be set if either are
    assert bool(job.action_commit) == bool(job.action_repo_url)

    input_files = []
    for action in job.requires_outputs_from:
        for filename in list_outputs_from_action(job.workspace, action):
            input_files.append(filename)

    outputs = {}
    for privacy_level, named_patterns in job.output_spec.items():
        for name, pattern in named_patterns.items():
            outputs[pattern] = privacy_level

    if job.cancelled:
        job_definition_cancelled = "user"
    else:
        job_definition_cancelled = None

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
        allow_database_access=allow_database_access,
        database_name=job.database_name if allow_database_access else None,
        # in future, these may come from the JobRequest, but for now, we have
        # config defaults.
        cpu_count=config.DEFAULT_JOB_CPU_COUNT,
        memory_limit=config.DEFAULT_JOB_MEMORY_LIMIT,
        level4_max_filesize=config.LEVEL4_MAX_FILESIZE,
        level4_max_csv_rows=config.LEVEL4_MAX_CSV_ROWS,
        level4_file_types=config.LEVEL4_FILE_TYPES,
        cancelled=job_definition_cancelled,
    )


def main():
    """Run the main run loop after starting the sync loop in a thread."""
    # extra space to align with other thread's "sync" label.
    from jobrunner.service import (
        ensure_valid_db,
        maintenance_wrapper,
        record_stats_wrapper,
        start_thread,
    )

    threading.current_thread().name = "agnt"
    fmt = "{asctime} {threadName} {message} {tags}"
    configure_logging(fmt)
    tracing.setup_default_tracing()

    # check db is present and up to date, or else error
    ensure_valid_db()

    try:
        log.info("jobrunner.agent started")
        # note: thread name appears in log output, so its nice to keep them all the same length
        start_thread(record_stats_wrapper, "stat")
        if config.ENABLE_MAINTENANCE_MODE_THREAD:
            start_thread(maintenance_wrapper, "mntn")

        agent()
    except KeyboardInterrupt:
        log.info("jobrunner.agent stopped")


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
