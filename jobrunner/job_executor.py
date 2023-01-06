from dataclasses import dataclass
from enum import Enum
from typing import List, Mapping, Optional


class Privacy(Enum):
    HIGH = "high"
    MEDIUM = "medium"


@dataclass
class Study:
    git_repo_url: str
    commit: str


@dataclass
class JobDefinition:
    id: str  # a unique identifier for the job  # noqa: A003
    job_request_id: str  # a unique identifier for the job's job request
    study: Study  # the study defining the action for this job
    workspace: str  # the workspace to run the job in
    action: str  # the name of the action that the job is running
    created_at: int  # UNIX timestamp, time job created
    image: str  # the Docker image to run
    args: List[str]  # the arguments to pass to the Docker container
    env: Mapping[str, str]  # the environment variables to set for the Docker container
    inputs: List[str]  # the files that the job requires
    output_spec: Mapping[
        str, str
    ]  # the files that the job should produce (globs mapped to privacy levels)
    allow_database_access: bool  # whether this job should have access to the database
    cpu_count: str = None  # number of CPUs to be allocated
    memory_limit: str = None  # memory limit to apply


class ExecutorState(Enum):
    PREPARING = "preparing"
    PREPARED = "prepared"
    EXECUTING = "executing"
    EXECUTED = "executed"
    FINALIZING = "finalizing"
    FINALIZED = "finalized"
    UNKNOWN = "unknown"
    ERROR = "error"


@dataclass
class JobStatus:
    state: ExecutorState
    message: Optional[str] = None
    timestamp_ns: int = (
        None  # timestamp this JobStatus occurred, in integer nanoseconds
    )


@dataclass
class JobResults:
    outputs: Mapping[str, str]  # mapping of outputs to privacy levels
    unmatched_patterns: List[str]  # list of patterns that matched no outputs
    unmatched_outputs: List[str]  # outputs that not match the output_spec
    exit_code: int
    image_id: str
    message: str = None
    # timestamp these results were finalized, in integer nanoseconds
    timestamp_ns: int = None

    # to be extracted from the image labels
    action_version: str = "unknown"
    action_revision: str = "unknown"
    action_created: str = "unknown"
    base_revision: str = "unknown"
    base_created: str = "unknown"


class ExecutorRetry(Exception):
    """Indicates to the job scheduler that there's a temporary issue and to try again later."""

    pass


class ExecutorAPI:
    """
    API for managing job execution.

    This API is called by the job-runner to manage each job it is tracking. It models the running of a job as a state
    machine, and the methods below are the transitions between states.

    They should return either:
     - the next state if successful
     - the ERROR state with an appropriate message if something has gone wrong.
     - the current state if is different from the expected state, with a message
     - the initial state to indicate back pressure and to retry later.

    Given the long running nature of jobs, it is an asynchronous API, and calls should not block for a more than a few
    seconds.

    All the state transition methods (prepare(), execute(), finalize(), terminate(), cleanup()) must be idempotent. If
    the relevant task they are responsible for is already running for that job, they must not start a new task, and
    instead return successfully the current state. This is best acheived by a guard to check the job's state, and
    return its current state if in the state you would expect. It's the responsibility of the job-runner scheduler to
    figure out what to do in this case.

    """

    def prepare(self, job: JobDefinition) -> JobStatus:
        """
        Launch a prepare task for a job, transitioning to the initial PREPARING state.

        1. Validate the JobDefinition. If there are errors, return an ERROR state with message.

        2. Check the job is currently in UNKNOWN state. If not return its current state, and the job-runner will handle
           the unexpected transition.

        3. Check the resources are available to prepare the job. If not, return the UNKNOWN state with an appropriate
           message.

        4. Create an ephemeral workspace to use for executing this job. This is expected to be a volume mounted into the
           container, but other implementations are allowed.

        5. Launch a prepare task asynchronously. If launched successfully, return the PREPARING state. If not, return an
           ERROR state with message.

        The prepare task must do the following:

          - check out the supplied study repo via the OpenSAFELY github proxy into the ephemeral workspace, erroring if
            there are any failures.
          - copying the supplied file inputs from the long-term workspace storage into the ephemeral workspace, erroring
            if there are any missing.

        When the prepare task finishes, the get_status() call should now return PREPARED for this job.

        This method must be idempotent. If called with a job that is already running a prepare task, it must not
        launch a new task, and simply return successfully with PREPARING.

        """

    def execute(self, job: JobDefinition) -> JobStatus:
        """
        Launch the execution of a job that has been prepared, transitioning from PREPARED to EXECUTING.

        1. Check the job is in the PREPARED state. If not, return its current state, and the job-runner will handle the
           unexpected transition.

        2. Validate that the ephemeral workspace created by prepare for this job exists.  If not, return an ERROR
           state with message.

        3. Check there are resources available to execute the job. If not, return PREPARED status with an appropriate
           message.

        4. Launch the job execution task asynchronously. If launched successfully, return the EXECUTING state. If not,
           return an ERROR state with message.

        The execution task must do the following:

        The specified image must be run, with the provided arguments and environment variables. The implementation
        may add environment variables to those in the job definition as necessary for the backend.

        The job should be run without any network access, unless definition.allow_database_access is set to True,
        in which case it should be run with a network allowing access to the database and any configuration needed to
        contact and authenticate with the database should be provided as environment variables.

        The job must be run with the ephemeral workspace for this job at /workspace in the filesystem.

        When the execute task finishes, the get_status() call must now return EXECUTED for this job.

        This method must be idempotent. If called with a job that is already running an execute task, it must not
        launch a new task, and simply return successfully with EXECUTING.

        """

    def finalize(self, job: JobDefinition) -> JobStatus:
        """
        Launch the finalization of a job, transitioning from EXECUTED to FINALIZING.

        1. Check the job is in the EXECUTED state. If not, return its current state, and the job-runner will handle the
           unexpected transition.

        2. Validate that the job's ephemeral workspace exists. If not, return an ERROR state with message.

        3. Launch the finalize task asynchronously. If launched successfully, return the FINALIZING state. If not,
           return an ERROR state with message.

        The finalize task should do the following:

        Any files that the job produced that match the output spec in the definition must be copied from the ephemeral
        workspace to the workspace long-term storage. Anything written by the container to stdout or stderr must be
        captured and written to a log file, metadata/{action}.log, in the workspace in long-term storage.

        The action log file and any files in the output spec marked as medium privacy must also be made available in the
        medium privacy view of the workspace in long-term storage.

        The action log file and any useful metadata from the job run should also be written to a separate log storage
        area in long-term storage.

        When the finalize task finishes, the get_status() call should now return FINALIZED for this job, and
        get_results() call should return the JobResults for this job.

        This method must be idempotent. If called with a job that is already running an finalize task, it must not
        launch a new task, and simply return successfully with FINALIZING.

        """

    def terminate(self, job: JobDefinition) -> JobStatus:
        """
        Terminate a running job, transitioning to the ERROR state.

        1. If any task for this job is running, terminate it, do not wait for it to complete.

        2. Return ERROR state with a message.

        """

    def cleanup(self, job: JobDefinition) -> JobStatus:
        """
        Clean up any remaining state for a finished job, transitioning to the UNKNOWN state.

        1. Initiate the cleanup, do not wait for it to complete.

        2. Return the UNKNOWN status.

        This method must be idempotent; it will be called at least once for every finished job. The implementation
        may defer resource cleanup to this method if necessary in order to correctly implement idempotency of
        get_status() or get_results(). If the job is unknown, it should still return UNKNOWN successfully.

        This method will not be called for a job that raises an unexpected exception from ExecutorAPI in order
        to facilitate debugging of unexpected failures. It may therefore be necessary for the backend to provide
        out-of-band mechanisms for cleaning up resources associated with such failures.
        """

    def get_status(self, job: JobDefinition) -> JobStatus:
        """
        Return the current status of a job.

        1. Check the job is known to the system. If not, return the UNKNOWN state.

        2. Return the current state of the job from the executors perspective.

        This should return a JobStatus with the appropriate state for the job. It is polled by job-runner to track the
        completion of the various tasks.

        This method must be idempotent; it may be called more than once for a job even after it has finished, so any
        irreversible cleanup which loses information about must be deferred to ExecutorAPI.cleanup() which will only be
        called once the results have been persisted.

        """

    def get_results(self, job: JobDefinition) -> JobResults:
        """
        Return the finalized results for a job.

        The results must include a list of output files that the job produced which matched its output spec. It
        should also include a list of files that it produced but which did not match the output spec, to aid in
        debugging during study development.

        This method must be idempotent; it may be called more than once for a job even after it has finished, so any
        irreversible cleanup which loses information about must be deferred to ExecutorAPI.cleanup() which will only be
        called once the results have been persisted.
        """

    def delete_files(self, workspace: str, privacy: Privacy, paths: [str]) -> List[str]:
        """
        Delete files from a workspace.

        This method must be idempotent; if any of the files specified doesn't exist then it must ignore them.

        Returns a list of any files that were present but it errored trying to delete them.
        """
        ...


class NullExecutorAPI(ExecutorAPI):
    """Null implementation of ExecutorAPI."""

    def prepare(self, job):
        raise NotImplementedError

    def execute(self, job):
        raise NotImplementedError

    def finalize(self, job):
        raise NotImplementedError

    def terminate(self, job):
        raise NotImplementedError

    def get_status(self, job):
        raise NotImplementedError

    def get_results(self, job):
        raise NotImplementedError

    def cleanup(self, job):
        raise NotImplementedError

    def delete_files(self, workspace, privacy, paths):
        raise NotImplementedError
