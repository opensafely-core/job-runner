import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from enum import Enum


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
    # a unique identifier for the task associated with this job. Note that a
    # job definition is constructed for use with a specific task; it may be
    # constructed multiple times for the same job (e.g. run the job, cancel the
    # job, run it again), each time with a  different task ID
    task_id: str
    study: Study  # the study defining the action for this job
    workspace: str  # the workspace to run the job in
    action: str  # the name of the action that the job is running
    created_at: int  # UNIX timestamp, time job created
    image: str  # the Docker image to run
    args: list[str]  # the arguments to pass to the Docker container
    env: Mapping[str, str]  # the environment variables to set for the Docker container
    inputs: list[
        str
    ]  # deprecated, previously a list of input files that this job requires
    input_job_ids: list[str]  # the ids of jobs that this job requires
    output_spec: Mapping[
        str, str
    ]  # the files that the job should produce (globs mapped to privacy levels)
    allow_database_access: bool  # whether this job should have access to the database
    level4_max_csv_rows: int
    level4_max_filesize: int
    # our internal name for the database this job uses (actual connection details are
    # passed in `env`)
    database_name: str = None
    cpu_count: str = None  # number of CPUs to be allocated
    memory_limit: str = None  # memory limit to apply
    level4_file_types: list = field(default_factory=lambda: [".csv"])

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        # Create the nested Study instance
        study_data = data.pop("study", {})
        study = Study(
            git_repo_url=study_data.get("git_repo_url"), commit=study_data.get("commit")
        )
        # Ensure any dict used to construct a JobDefinition has a task_id key
        if "task_id" not in data:
            data["task_id"] = ""

        # Create the JobDefinition instance with the Study object
        return cls(study=study, **{k: v for k, v in data.items()})


class ExecutorState(Enum):
    # Job is currently preparing to run: creating volumes,copying files, etc
    PREPARING = "preparing"
    # Job volume is prepared and ready to run
    PREPARED = "prepared"
    # Job currently executing
    EXECUTING = "executing"
    # Job process has finished executing, and has an exit code
    EXECUTED = "executed"
    # Job is currently being inspected and finalized
    FINALIZING = "finalizing"
    # Job has finished finalization
    FINALIZED = "finalized"
    # Executor doesn't know anything about this job (it only tracks active jobs)
    UNKNOWN = "unknown"
    # There was an error with the executor (*not* the same thing as an error with job)
    ERROR = "error"


@dataclass
class JobStatus:
    state: ExecutorState

    # timestamp this JobStatus occurred, in integer nanoseconds
    timestamp_ns: int = field(default_factory=time.time_ns)
    results: dict = field(default_factory=dict)


class ExecutorRetry(Exception):
    """Indicates to the job scheduler that there's a temporary issue and to try again later."""

    pass


class ExecutorAPI:
    """
    API for managing job execution.

    This API is called by the job-runner to manage each job it is tracking. It models the running of a job as a state
    machine, and the methods below are the transitions between states.

    Given the long running nature of jobs, it is an asynchronous API, and calls should not block for a more than a few
    seconds.

    All the state transition methods (prepare(), execute(), finalize(), terminate(), cleanup()) must be idempotent. If
    the relevant task they are responsible for is already running for that job, they must not start a new task, and
    instead return successfully the current state. This is best acheived by a guard to check the job's state, and
    return its current state if in the state you would expect. It's the responsibility of the job-runner scheduler to
    figure out what to do in this case.

    """

    def prepare(self, job_definition: JobDefinition) -> None:
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

    def execute(self, job_definition: JobDefinition) -> None:
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

        The job should be run without any network access, unless job_definition.allow_database_access is set to True,
        in which case it should be run with a network allowing access to the database and any configuration needed to
        contact and authenticate with the database should be provided as environment variables.

        The job must be run with the ephemeral workspace for this job at /workspace in the filesystem.

        When the execute task finishes, the get_status() call must now return EXECUTED for this job.

        This method must be idempotent. If called with a job that is already running an execute task, it must not
        launch a new task, and simply return successfully with EXECUTING.

        """

    def finalize(
        self,
        job_definition: JobDefinition,
        cancelled: bool = False,
        error: dict | None = None,
    ) -> None:
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

        If the job has been cancelled, it should only preserve the action log file.

        When the finalize task finishes, the get_status() call should now return FINALIZED for this job.

        This method must be idempotent. If called with a job that is already running an finalize task, it must not
        launch a new task, and simply return successfully with FINALIZING.

        """

    def terminate(self, job_definition: JobDefinition) -> None:
        """
        Terminate a running job, transitioning to the EXECUTED state.

        1. If any task for this job is running, terminate it, do not wait for it to complete.

        2. Return EXECUTED state with a message.

        Terminating a running job is considered an expected state, not an error state. This decision
        also makes it easier for current executor implementations to cleanup after termination, and
        is consistent with the handling of programs that exit of their own accord with a return code.

        """

    def cleanup(self, job_definition: JobDefinition) -> None:
        """
        Clean up any remaining state for a finished job, transitioning to the UNKNOWN state.

        1. Initiate the cleanup, do not wait for it to complete.

        2. Return the UNKNOWN status.

        This method must be idempotent; it will be called at least once for every finished job. The implementation
        may defer resource cleanup to this method if necessary in order to correctly implement idempotency of
        get_status(). If the job is unknown, it should still return UNKNOWN successfully.

        This method will not be called for a job that raises an unexpected exception from ExecutorAPI in order
        to facilitate debugging of unexpected failures. It may therefore be necessary for the backend to provide
        out-of-band mechanisms for cleaning up resources associated with such failures.
        """

    def get_status(self, job_definition: JobDefinition, cancelled=False) -> JobStatus:
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

    def delete_files(self, workspace: str, privacy: Privacy, files: [str]) -> list[str]:
        """
        Delete files from a workspace.

        This method must be idempotent; if any of the files specified doesn't exist then it must ignore them.

        Returns a list of any files that were present but it errored trying to delete them.
        """
