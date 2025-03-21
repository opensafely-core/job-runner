"""
Script which polls the database for active (i.e. non-terminated) jobs, takes
the appropriate action for each job depending on its current state, and then
updates its state as appropriate.
"""

import collections
import datetime
import logging
import os
import sys
import time
from typing import Optional

from opentelemetry import trace

from jobrunner import config, tracing
from jobrunner.controller import mark_job_as_failed, set_code
from jobrunner.executors import get_executor_api
from jobrunner.job_executor import (
    ExecutorAPI,
    ExecutorRetry,
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


def main(exit_callback=lambda _: False):
    log.info("jobrunner.agent loop started")
    api = get_executor_api()

    while True:
        with tracer.start_as_current_span("LOOP", attributes={"loop": True}):
            handle_running_jobs(api)

        time.sleep(config.JOB_LOOP_INTERVAL)


def handle_running_jobs(api: Optional[ExecutorAPI]):
    running_jobs = [
        j
        for j in find_where(Job, state=State.RUNNING)
        if j.status_code != StatusCode.FINALIZED
    ]

    running_for_workspace = collections.defaultdict(int)
    handled_jobs = []

    for job in running_jobs:
        # `set_log_context` ensures that all log messages triggered anywhere
        # further down the stack will have `job` set on them
        with set_log_context(job=job):
            handle_single_job(job, api)

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


def handle_single_job(job, api):
    """The top level handler for a job.

    Mainly exists to wrap the job handling in an exception handler.
    """
    # we re-read the flags before considering each job, so make sure they apply
    # as soon as possible when set.
    mode = get_flag_value("mode")
    paused = str(get_flag_value("paused", "False")).lower() == "true"
    try:
        synchronous_transition = trace_handle_job(job, api, mode, paused)

        # provide a way to shortcut moving a job on to the next state right away
        # this is intended to support executors where some state transitions
        # are synchronous, particularly the local executor where prepare is
        # synchronous and can be time consuming.
        if synchronous_transition:
            trace_handle_job(job, api, mode, paused)
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


def trace_handle_job(job, api, mode, paused):
    """Call handle job with tracing."""
    attrs = {
        "initial_state": job.state.name,
        "initial_code": job.status_code.name,
    }

    with tracer.start_as_current_span("LOOP_JOB") as span:
        tracing.set_span_metadata(span, job, **attrs)
        try:
            synchronous_transition = handle_running_job(job, api)
        except Exception as exc:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
            span.record_exception(exc)
            raise
        else:
            span.set_attribute("final_state", job.state.name)
            span.set_attribute("final_code", job.status_code.name)

    return synchronous_transition


def handle_running_job(job, api, mode=None, paused=None):
    """Handle an active job.

    This contains the main state machine logic for a job. For the most part,
    state transitions follow the same logic, which is abstracted. Some
    transitions require special logic, mainly the initial and final states, as
    well as supporting cancellation and various operational modes.
    """
    assert job.state in (State.PENDING, State.RUNNING)
    job_definition = job_to_job_definition(job)

    # does this api have synchronous_transitions?
    synchronous_transitions = getattr(api, "synchronous_transitions", [])
    is_synchronous = False

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
            set_code(job, new_statuscode, "Cancelled whilst executing")
            return
        if initial_status.state == ExecutorState.PREPARED:
            set_code(
                job,
                StatusCode.FINALIZED,
                "Cancelled whilst prepared",
            )
            # Nb. no need to actually run finalize() in this case
            return

    # only consider db maintenance if not already cancelled
    elif mode == "db-maintenance" and job_definition.allow_database_access:
        log.warning(f"DB maintenance mode active, killing db job {job_definition.id}")
        # we ignore the JobStatus returned from these API calls, as this is not a hard error
        api.terminate(job_definition)
        api.cleanup(job_definition)

        # reset state to pending and exit
        set_code(
            job,
            StatusCode.WAITING_DB_MAINTENANCE,
            "Waiting for database to finish maintenance",
        )
        return

    # check if we've transitioned since we last checked and trace it.
    if initial_status.state in STATE_MAP:
        initial_code, initial_message = STATE_MAP[initial_status.state]
        if initial_code != job.status_code:
            set_code(
                job,
                initial_code,
                initial_message,
                timestamp_ns=initial_status.timestamp_ns,
            )

    # handle the simple no change needed states.
    if initial_status.state in STABLE_STATES:
        # no action needed, simply update job message and timestamp, which is likely a no-op
        code, message = STATE_MAP[initial_status.state]
        set_code(job, code, message)
        return

    if initial_status.state == ExecutorState.ERROR:
        # something has gone wrong since we last checked
        raise ExecutorError(initial_status.message)

    # ok, handle the state transitions that are our responsibility
    if initial_status.state == ExecutorState.UNKNOWN:
        if job.status_code != StatusCode.READY:
            log.warning("state error: got UNKNOWN state for a job")

        if ExecutorState.PREPARING in synchronous_transitions:
            # prepare is synchronous, which means set our code to PREPARING
            # before calling  api.prepare(), and we expect it to be PREPARED
            # when finished
            code, message = STATE_MAP[ExecutorState.PREPARING]
            set_code(job, code, message)
            expected_state = ExecutorState.PREPARED
            is_synchronous = True
        else:
            expected_state = ExecutorState.PREPARING

        new_status = api.prepare(job_definition)

    elif initial_status.state == ExecutorState.PREPARED:
        expected_state = ExecutorState.EXECUTING
        new_status = api.execute(job_definition)

    elif initial_status.state == ExecutorState.EXECUTED:
        if ExecutorState.FINALIZING in synchronous_transitions:
            # finalize is synchronous, which means set our code to FINALIZING
            # before calling  api.finalize(), and we expect it to be FINALIZED
            # when finished
            code, message = STATE_MAP[ExecutorState.FINALIZING]
            set_code(job, code, message)
            expected_state = ExecutorState.FINALIZED
            # the agent loop does not do anything beyond FINALIZED now, so don't jump
            is_synchronous = False
        else:
            expected_state = ExecutorState.FINALIZING

        new_status = api.finalize(job_definition)

    if new_status.state == expected_state:
        # successful state change to the expected next state
        code, message = STATE_MAP[new_status.state]
        set_code(job, code, message)

        # we want to immediately run this function for this job again to
        # avoid blocking it as we know the state transition has already
        # completed.
        return is_synchronous

    elif new_status.state == ExecutorState.ERROR:
        # all transitions can go straight to error
        raise ExecutorError(new_status.message)

    else:
        raise InvalidTransition(
            f"unexpected state transition of job {job.id} from {initial_status.state} to {new_status.state}: {new_status.message}"
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


if __name__ == "__main__":
    configure_logging()

    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
