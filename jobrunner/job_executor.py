from dataclasses import dataclass
from enum import Enum
from typing import Protocol, Mapping, List, Tuple, Optional

from jobrunner.models import State, StatusCode


class Privacy(Enum):
    HIGH = "high"
    MEDIUM = "medium"


InputSpec = Mapping[str, str]  # file paths to actions that produced them
OutputSpec = Mapping[str, Mapping[str, str]]  # privacy levels to names to patterns
Study = Tuple[str, str]  # Git repo and commit


@dataclass
class JobDefinition:
    study: Study  # the study defining the action for this job
    workspace: str  # the workspace to run the job in
    action: str  # the name of the action that the job is running
    image: str  # the Docker image to run
    args: List[str]  # the arguments to pass to the Docker container
    env: Mapping[str, str]  # the environment variables to set for the Docker container
    inputs: InputSpec  # the files that the job requires, a mapping of file paths to the actions that produced them
    outputs: OutputSpec  # a description of the expected outputs (privacy level mapped to patterns)
    allow_database_access: bool  # whether this job should have access to the database


@dataclass
class JobResults:
    state: State
    status_code: Optional[StatusCode]
    status_message: str
    outputs: Mapping[str, str]
    unmatched_outputs: List[str]


class JobAPI(Protocol):
    def run(self, job_id: str, definition: JobDefinition) -> None:
        """
        Run a job.

        The job should be run without any network access, unless allow_database_access is set to True, in which case it
        should be run with a network allowing access to the database and any configuration needed to contact and
        authenticate with the database should be provided as environment variables.

        The implementation may add environment variables to those in the job definition as necessary for the backend.

            Parameters:
                job_id (str): the id of the job
                definition (JobDefinition): the definition of the job

            Raises:
                JobError: if the job definition is invalid
        """
        ...

    def terminate(self, job_id):
        ...

    def get_status(self, job_id: str, workspace: str, action: str) -> Tuple[State, Optional[JobResults]]:
        """
        Return the status of a job and the results if it has finished.

            Parameters:
                job_id (str): the job
                workspace (str): the workspace that the job is running in
                action (str): the action that the job is running

            Returns:
                state (State): the state of the job
                results (Optional[JobResults]): the results, if the job has finished

            Raises:
                JobError: if there is a problem retrieving the status of the job
         """
        ...


class WorkspaceAPI(Protocol):
    def delete_files(self, workspace, privacy, paths):
        ...
