from dataclasses import dataclass
from enum import Enum
from typing import Mapping, List, Tuple, Optional

from jobrunner.models import State, StatusCode
from jobrunner import config


class Privacy(Enum):
    HIGH = "high"
    MEDIUM = "medium"


@dataclass
class Study:
    git_repo_url: str
    commit: str


@dataclass
class JobDefinition:
    id: str  # a unique identifier for the job
    study: Study  # the study defining the action for this job
    workspace: str  # the workspace to run the job in
    action: str  # the name of the action that the job is running
    image: str  # the Docker image to run
    args: List[str]  # the arguments to pass to the Docker container
    env: Mapping[str, str]  # the environment variables to set for the Docker container
    inputs: List[str]  # the files that the job requires
    output_spec: Mapping[
        str, str
    ]  # the files that the job should produce (globs mapped to privacy levels)
    allow_database_access: bool  # whether this job should have access to the database


@dataclass
class JobResults:
    state: State
    status_code: Optional[StatusCode]
    status_message: str
    outputs: Mapping[str, str]
    exit_code: int
    image_id: str


class JobAPI:
    def run(self, job: JobDefinition) -> None:
        """
        Run a job.

        This method must be idempotent; it may be called more than once with the same job_id, in which case only one
        job should be created. It must also be idempotent in the face of errors; if it throws an exception because
        job creation has failed due to a transient error it may be called again to retry the operation and this
        should succeed if possible. (The implementation may provide a configuration option which breaks this
        idempotency by preserving resources after a failure to aid debugging.)

        The specified image must be run, with the provided arguments and environment variables. The implementation
        may add environment variables to those in the job definition as necessary for the backend.

        The job should be run without any network access, unless definition.allow_database_access is set to True,
        in which case it should be run with a network allowing access to the database and any configuration needed to
        contact and authenticate with the database should be provided as environment variables.

        The job must be run with a workspace directory at /workspace in the filesystem (this is expected to be a
        volume mounted into the container, but other implementations are allowed). The workspace must contain a
        checkout of the study and any inputs specified in the job definition, copied from the workspace in long-term
        storage. If any of the specified inputs is not present in long-term storage then a JobError must be raised with
        details of the missing file.

        Any files that the job produces that match the output spec in the definition must be copied to the workspace
        long-term storage. Anything written by the container to stdout or stderr must be captured and written to a
        log file, metadata/{action}.log, in the workspace in long-term storage.

        The action log file and any files in the output spec marked as medium privacy must also be made available in the
        medium privacy view of the workspace in long-term storage.

        The action log file and any useful metadata from the job run should also be written to a separate log storage
        area in long-term storage.

            Raises:
                JobError: if the job definition is invalid or job creation fails
        """
        ...

    def terminate(self, job: JobDefinition) -> None:
        """
        Terminate a running job.

        This method must be idempotent; it may be called for a job that doesn't exist or which has already been
        terminated in which case it must return silently.

            Raises:
                JobError: if job termination fails
        """
        ...

    def get_status(self, job: JobDefinition) -> Tuple[State, Optional[JobResults]]:
        """
        Return the status of a job and the results if it has finished.

        This method must be idempotent; it may be called more than once for a job even after it has finished, so any
        irreversible cleanup which loses information about must be deferred to JobAPI.cleanup() which will only be
        called once the results have been persisted.

        If no matching job can be found, it should raise a JobError exception.

        The results must include a list of output files that the job produced which matched its output spec. It
        should also include a list of files that it produced but which did not match the output spec,
        to aid in debugging during study development.

            Returns:
                state (State): the state of the job
                results (Optional[JobResults]): the results, if the job has finished

            Raises:
                JobError: if there is a problem retrieving the status of the job
        """
        ...

    def cleanup(self, job: JobDefinition) -> None:
        """
        Clean up any remaining state for a finished job.

        This method must be idempotent; it will be called at least once for every finished job. The implementation
        may defer resource cleanup to this method if necessary in order to correctly implement idempotency of
        JobAPI.get_status().

        This method will not be called for a job that raises an unexpected exception from JobAPI.get_status() (anything
        other than JobError), in order to facilitate debugging of unexpected failures. It may therefore be necessary
        for the backend to provide out-of-band mechanisms for cleaning up resources associated with such failures.
        """
        ...


class WorkspaceAPI:
    def delete_files(self, workspace: str, privacy: Privacy, paths: [str]):
        """
        Delete files from a workspace.

        This method must be idempotent; if any of the files specified doesn't exist then it must ignore them.
        """
        ...


class NullJobAPI(JobAPI):
    """Null implementation of JobAPI."""

    def run(self, job: JobDefinition) -> None:
        raise NotImplemented

    def terminate(self, job: JobDefinition) -> None:
        raise NotImplemented

    def get_status(self, job: JobDefinition) -> Tuple[State, Optional[JobResults]]:
        raise NotImplemented

    def cleanup(self, job: JobDefinition) -> None:
        raise NotImplemented


class NullWorkspaceAPI:
    def delete_files(self, workspace: str, privacy: Privacy, paths: [str]):
        raise NotImplemented


def get_job_api():
    return NullJobAPI()


def get_workspace_api():
    return NullWorkspaceAPI()
