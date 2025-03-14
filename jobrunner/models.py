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
import hashlib
import secrets
import shlex
from enum import Enum
from functools import total_ordering

from jobrunner.lib.database import databaseclass, migration
from jobrunner.lib.string_utils import slugify


# this is the overall high level state the job-runner uses to decide how to
# handle a particular job.
class State(Enum):
    PENDING = "pending"
    RUNNING = "running"
    FAILED = "failed"
    SUCCEEDED = "succeeded"


# In contrast to State, these play no role in the state machine controlling
# what happens with a job. These are designed specifically for reporting the
# current low-level state of a job. They are simply machine readable versions
# of the human readable status_message which allow us to provide certain UX
# affordances in the web, cli and telemetry.


@total_ordering
class StatusCode(Enum):
    # PENDING states
    #
    # initial state of a job, not yet running
    CREATED = "created"
    # waiting for pause mode to exit
    WAITING_PAUSED = "paused"
    # waiting db maintenance mode to exit
    WAITING_DB_MAINTENANCE = "waiting_db_maintenance"
    # waiting on dependant jobs
    WAITING_ON_DEPENDENCIES = "waiting_on_dependencies"
    # waiting on available resources to run the job
    WAITING_ON_WORKERS = "waiting_on_workers"
    # waiting on available db resources to run the job
    WAITING_ON_DB_WORKERS = "waiting_on_db_workers"
    # reset for reboot
    WAITING_ON_REBOOT = "waiting_on_reboot"

    # RUNNING states, these mirror ExecutorState, and are the normal happy path
    PREPARING = "preparing"
    PREPARED = "prepared"
    EXECUTING = "executing"
    EXECUTED = "executed"
    FINALIZING = "finalizing"
    FINALIZED = "finalized"

    # SUCCEEDED states. Simples.
    SUCCEEDED = "succeeded"

    # FAILED states
    DEPENDENCY_FAILED = "dependency_failed"
    NONZERO_EXIT = "nonzero_exit"
    CANCELLED_BY_USER = "cancelled_by_user"
    UNMATCHED_PATTERNS = "unmatched_patterns"
    INTERNAL_ERROR = "internal_error"
    KILLED_BY_ADMIN = "killed_by_admin"
    STALE_CODELISTS = "stale_codelists"

    @property
    def is_final_code(self):
        return self in StatusCode._FINAL_STATUS_CODES

    def __lt__(self, other):
        order = list(self.__class__)
        return order.index(self) < order.index(other)


# used for tracing to know if a state is final or not
StatusCode._FINAL_STATUS_CODES = [
    StatusCode.SUCCEEDED,
    StatusCode.DEPENDENCY_FAILED,
    StatusCode.NONZERO_EXIT,
    StatusCode.CANCELLED_BY_USER,
    StatusCode.UNMATCHED_PATTERNS,
    StatusCode.INTERNAL_ERROR,
    StatusCode.KILLED_BY_ADMIN,
    StatusCode.STALE_CODELISTS,
]


# This is our internal representation of a JobRequest which we pass around but
# never save to the database (hence no __tablename__ attribute)
@dataclasses.dataclass
class JobRequest:
    id: str  # noqa: A003
    repo_url: str
    commit: str
    requested_actions: list
    cancelled_actions: list
    workspace: str
    codelists_ok: bool
    database_name: str
    force_run_dependencies: bool = False
    branch: str = None
    original: dict = None


# This stores the original JobRequest as received from the job-server. Once
# we've created the relevant Jobs we have no real need for the JobRequest
# object, but we it's useful to store it for debugging/audit purposes so we
# just save a blob of the original JSON as received from the job-server.
@databaseclass
class SavedJobRequest:
    __tablename__ = "job_request"
    __tableschema__ = """
        CREATE TABLE job_request (
            id TEXT,
            original TEXT,
            PRIMARY KEY (id)
        );
    """

    id: str  # noqa: A003
    original: dict


@databaseclass
class Job:
    __tablename__ = "job"
    __tableschema__ = """
        CREATE TABLE job (
            id TEXT,
            job_request_id TEXT,
            state TEXT,
            repo_url TEXT,
            "commit" TEXT,
            workspace TEXT,
            database_name TEXT,
            action TEXT,
            action_repo_url TEXT,
            action_commit TEXT,
            requires_outputs_from TEXT,
            wait_for_job_ids TEXT,
            run_command TEXT,
            image_id TEXT,
            output_spec TEXT,
            outputs TEXT,
            unmatched_outputs TEXT,
            status_message TEXT,
            status_code TEXT,
            cancelled BOOLEAN,
            created_at INT,
            updated_at INT,
            started_at INT,
            completed_at INT,
            trace_context TEXT,
            status_code_updated_at INT,
            level4_excluded_files TEXT,
            requires_db BOOLEAN,

            PRIMARY KEY (id)
        );

        CREATE INDEX idx_job__job_request_id ON job (job_request_id);

        -- Once jobs transition into a terminal state (failed or succeeded) they become
        -- basically irrelevant from the application's point of view as it never needs
        -- to query them. By creating an index only on non-terminal states we ensure
        -- that it always stays relatively small even as the set of historical jobs
        -- grows.
        CREATE INDEX idx_job__state ON job (state) WHERE state NOT IN ('failed', 'succeeded');
    """

    migration(
        1,
        """
        ALTER TABLE job ADD COLUMN trace_context TEXT;
        ALTER TABLE job ADD COLUMN status_code_updated_at INT;
        """,
    )

    migration(
        2,
        """
        ALTER TABLE job ADD COLUMN level4_excluded_files TEXT;
        """,
    )

    migration(
        3,
        """
        ALTER TABLE job ADD COLUMN requires_db BOOLEAN;
        """,
    )

    id: str = None  # noqa: A003
    job_request_id: str = None
    state: State = None
    # Git repository URL
    repo_url: str = None
    # Full commit sha
    commit: str = None
    # Name of workspace (effectively, the output directory)
    workspace: str = None
    # Only applicable to "generate_cohort" jobs: the name of the database to
    # query against
    database_name: str = None
    # Name of the action (one of the keys in the `actions` dict in
    # project.yaml)
    action: str = None
    # URL of git repository for action (None if action is not reusable)
    action_repo_url: str = None
    # Full SHA of commit in action repo (None if action is not reusable)
    action_commit: str = None
    # List of action names whose outputs need to be used as inputs to this
    # action
    requires_outputs_from: list = None
    # List of job IDs we need to wait to finish before we can run (these will
    # represent the subset of the actions above which hadn't already run when
    # this job was scheduled)
    wait_for_job_ids: list = None
    # The docker run arguments to execute
    run_command: str = None
    # The specific docker image that was actually run
    image_id: str = None
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
    # Flag indicating that the user has cancelled this job
    cancelled: bool = False
    # Times (stored as integer UNIX timestamps in seconds)
    created_at: int = None
    updated_at: int = None
    started_at: int = None
    completed_at: int = None

    # Note: this timestamp should be in nanoseconds, not seconds
    status_code_updated_at: int = None
    # used to track the OTel trace context for this job
    trace_context: dict = None

    # map of file -> error
    level4_excluded_files: dict = None

    # does the job require db access
    requires_db: bool = False

    # used to cache the job_request json by the tracing code
    _job_request = None

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
            self.id = deterministic_id(f"{self.job_request_id}\n{self.action}")

    def asdict(self):
        data = dataclasses.asdict(self)
        for key, value in data.items():
            # Convert Enums to strings for straightforward JSON serialization
            if isinstance(value, Enum):
                data[key] = value.value
            # Convert UNIX timestamp to ISO format
            elif isinstance(value, int) and key.endswith("_at"):
                if key == "status_code_updated_at":
                    value /= 1e9
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
    def slug(self):
        """
        Use a human-readable slug rather than just an opaque ID to identify jobs in
        order to make debugging easier
        """
        return slugify(f"{self.workspace}-{self.action}-{self.id}")

    @property
    def output_files(self):
        if self.outputs:
            return self.outputs.keys()
        else:
            return []

    @property
    def action_args(self):
        if self.run_command:
            return shlex.split(self.run_command)
        else:
            return []


def deterministic_id(seed):
    digest = hashlib.sha1(seed.encode("utf-8")).digest()
    return base64.b32encode(digest[:10]).decode("ascii").lower()


def random_id():
    return secrets.token_hex(5)


def timestamp_to_isoformat(ts):
    if ts is None:
        return None
    return datetime.datetime.utcfromtimestamp(ts).isoformat() + "Z"


def isoformat_to_timestamp(string):
    return int(
        datetime.datetime.fromisoformat(string.rstrip("Z") + "+00:00")
        .astimezone(datetime.timezone.utc)
        .timestamp()
    )


@databaseclass
class Flag:
    __tablename__ = "flags"
    __tableschema__ = """
        CREATE TABLE flags (
            id TEXT,
            value TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        )
    """

    id: str  # noqa: A003
    value: str
    timestamp: int = None

    @property
    def timestamp_isoformat(self):
        return timestamp_to_isoformat(self.timestamp)

    def __str__(self):
        ts = self.timestamp_isoformat if self.timestamp else "never set"
        return f"{self.id}={self.value} ({ts})"
