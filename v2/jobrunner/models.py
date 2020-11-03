"""
Defines the basic data structures which we pass around and store in the
database.

The `database` module contains some very basic code for storing/retrieving
these objects.

Note the schema is defined separately in `schema.sql`.
"""
import base64
import dataclasses
from enum import Enum
import secrets


class State(Enum):
    PENDING = "P"
    RUNNING = "R"
    FAILED = "F"
    COMPLETED = "C"


# This is our internal representation of a JobRequest which we pass around but
# never save to the database (hence no __tablename__ attribute)
@dataclasses.dataclass
class JobRequest:
    id: str
    repo_url: str
    commit: str
    action: str
    workspace: str
    database_name: str
    force_run: bool = False
    force_run_dependencies: bool = False
    branch: str = None
    original: dict = None


# This stores the original JobRequest as received from the job-server. We only
# store this so that we can include it with the job outputs for debugging/audit
# purposes.
@dataclasses.dataclass
class SavedJobRequest:
    __tablename__ = "job_request"

    id: str
    original: dict


@dataclasses.dataclass
class Job:
    __tablename__ = "job"

    id: str
    job_request_id: str = None
    status: State = None
    repo_url: str = None
    commit: str = None
    workspace: str = None
    database_name: str = None
    action: str = None
    wait_for_job_ids: list = None
    requires_outputs_from: list = None
    run_command: str = None
    output_spec: dict = None
    output_files: dict = None
    error_message: str = None

    def asdict(self):
        data = dataclasses.asdict(self)
        for key, value in data.items():
            # Convert Enums to strings for straightforward JSON serialization
            if isinstance(value, Enum):
                data[key] = value.value
        return data

    @staticmethod
    def new_id():
        """
        Return a random 16 character lowercase alphanumeric string

        We used to use UUID4's but they are unnecessarily long for our purposes
        (particularly the hex representation) and shorter IDs make debugging
        and inspecting the job-runner a bit more ergonomic.
        """
        return base64.b32encode(secrets.token_bytes(10)).decode('ascii').lower()
