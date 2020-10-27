"""
Defines the basic data structures which we pass around and store in the
database.

The `database` module contains some very basic code for storing/retrieving
these objects.

Note the schema is defined separately in `schema.sql`.
"""
from dataclasses import dataclass
from enum import Enum


class State(Enum):
    PENDING = "P"
    RUNNING = "R"
    FAILED = "F"
    COMPLETED = "C"


# This is our internal representation of a JobRequest which we pass around but
# never save to the database (hence no __tablename__ attribute)
@dataclass
class JobRequest:
    id: str
    repo_url: str
    commit: str
    branch: str
    action: str
    workspace: str
    original: dict


# This stores the original JobRequest as received from the job-server. We only
# store this so that we can include it with the job outputs for debugging/audit
# purposes.
@dataclass
class SavedJobRequest:
    __tablename__ = "job_request"

    id: str
    original: dict


@dataclass
class Job:
    __tablename__ = "job"

    id: str
    job_request_id: str = None
    status: State = None
    repo_url: str = None
    commit: str = None
    workspace: str = None
    action: str = None
    wait_for_job_ids: list = None
    requires_outputs_from: list = None
    run_command: str = None
    output_spec: dict = None
    output_files: dict = None
    error_message: str = None
