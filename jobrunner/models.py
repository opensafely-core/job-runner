"""
Defines the basic data structures which we pass around and store in the
database.

The `database` module contains some very basic code for storing/retrieving
these objects.

Note the schema is defined separately in `schema.sql`.
"""
import base64
import dataclasses
import datetime
from enum import Enum
import hashlib

from .string_utils import slugify, project_name_from_url


class State(Enum):
    PENDING = "pending"
    RUNNING = "running"
    FAILED = "failed"
    SUCCEEDED = "succeeded"


# In contrast to State, these play no role in the state machine controlling
# what happens with a job. They are simply machine readable versions of the
# human readable status_message which allow us to provide certain UX
# affordances in the web and command line interfaces. These get added as we
# have a direct need for them, hence the minimal list below.
class StatusCode(Enum):
    WAITING_ON_DEPENDENCIES = "waiting_on_dependencies"
    DEPENDENCY_FAILED = "dependency_failed"
    WAITING_ON_WORKERS = "waiting_on_workers"
    NONZERO_EXIT = "nonzero_exit"


# This is our internal representation of a JobRequest which we pass around but
# never save to the database (hence no __tablename__ attribute)
@dataclasses.dataclass
class JobRequest:
    id: str
    repo_url: str
    commit: str
    requested_actions: list
    workspace: str
    database_name: str
    force_run_dependencies: bool = False
    force_run_failed: bool = False
    branch: str = None
    original: dict = None


# This stores the original JobRequest as received from the job-server. Once
# we've created the relevant Jobs we have no real need for the JobRequest
# object, but we it's useful to store it for debugging/audit purposes so we
# just save a blob of the original JSON as received from the job-server.
@dataclasses.dataclass
class SavedJobRequest:
    __tablename__ = "job_request"

    id: str
    original: dict


@dataclasses.dataclass
class Job:
    __tablename__ = "job"

    id: str = None
    job_request_id: str = None
    state: State = None
    # Git repository URL (may be a local path in LOCAL_RUN_MODE)
    repo_url: str = None
    # Full commit sha
    commit: str = None
    # Name of workspace (effictively, the output directory)
    workspace: str = None
    # Only applicable to "generate_cohort" jobs: the name of the database to
    # query against
    database_name: str = None
    # Name of the action (one of the keys in the `actions` dict in
    # project.yaml)
    action: str = None
    # List of action names whose outputs need to be used as inputs to this
    # action
    requires_outputs_from: list = None
    # List of job IDs we need to wait to finish before we can run (these will
    # represent the subset of the actions above which hadn't already run when
    # this job was scheduled)
    wait_for_job_ids: list = None
    # The docker run arguments to execute
    run_command: str = None
    # The specification of what outputs this job expects to produce, as a bunch
    # of named glob patterns organised by privacy level
    output_spec: dict = None
    # The outputs the job did produce matching the patterns above, as a mapping
    # of filenames to privacy levels
    outputs: dict = None
    # A list of the outputs the job produced which didn't match any of the
    # output patterns. This is only populated in the case that there are
    # unmatched output patterns, and is only used for debugging purposes.
    unmatched_outputs: list = None
    # Human readable string giving details about what's currently happening
    # with this job
    status_message: str = None
    # Machine readable code representing the status_message above
    status_code: StatusCode = None
    # Times (stored as integer UNIX timestamps)
    created_at: int = None
    updated_at: int = None
    started_at: int = None
    completed_at: int = None

    def __post_init__(self):
        # Generate a Job ID based on the Job Request ID and action. This means
        # we will always generate the same set of job IDs from a given Job
        # Request and so we won't create "orphan" jobs if we have to recreate
        # the job-runner database mid-job.
        #
        # Actions must be unique within a Job Request so this pair is
        # sufficient to give us global uniqueness. In fact we could do away
        # with Job IDs altogether and just use the action name directly, but
        # doing things this way is a less invasive change.
        if not self.id and self.job_request_id and self.action:
            hash_input = f"{self.job_request_id}\n{self.action}"
            hash_bytes = hashlib.sha1(hash_input.encode("utf-8")).digest()
            hash_token = base64.b32encode(hash_bytes[:10]).decode("ascii").lower()
            self.id = hash_token

    def asdict(self):
        data = dataclasses.asdict(self)
        for key, value in data.items():
            # Convert Enums to strings for straightforward JSON serialization
            if isinstance(value, Enum):
                data[key] = value.value
            # Convert UNIX timestamp to ISO format
            elif isinstance(value, int) and key.endswith("_at"):
                data[key] = timestamp_to_isoformat(value)
        return data

    @property
    def created_at_isoformat(self):
        return timestamp_to_isoformat(self.created_at)

    @property
    def updated_at_isoformat(self):
        return timestamp_to_isoformat(self.updated_at)

    @property
    def started_at_isoformat(self):
        return timestamp_to_isoformat(self.started_at)

    @property
    def completed_at_isoformat(self):
        return timestamp_to_isoformat(self.completed_at)

    # On Python 3.8 we could use `functools.cached_property` here and avoid
    # recomputing this every time
    @property
    def project(self):
        """Project name based on github url."""
        return project_name_from_url(self.repo_url)

    # On Python 3.8 we could use `functools.cached_property` here and avoid
    # recomputing this every time
    @property
    def slug(self):
        """
        Use a human-readable slug rather than just an opaque ID to identify jobs in
        order to make debugging easier
        """
        return slugify(f"{self.project}-{self.action}-{self.id}")


def timestamp_to_isoformat(ts):
    if ts is None:
        return None
    return datetime.datetime.utcfromtimestamp(ts).isoformat() + "Z"
