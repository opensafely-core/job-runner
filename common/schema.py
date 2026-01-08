import dataclasses
from dataclasses import dataclass
from enum import Enum


class TaskType(Enum):
    RUNJOB = "runjob"
    CANCELJOB = "canceljob"
    # TODO: delete job
    DBSTATUS = "dbstatus"
    DBDATACHECK = "dbdatacheck"

    @classmethod
    def from_value(cls, value):
        value_map = {item.value: item for item in cls}
        return value_map[value]


@dataclass(frozen=True)
class AgentTask:
    """Task API task data

    This is basically all the information about the task supplied by the
    controller, and is effectively immutable.

    The agent's view of a task's current state is computed on demand from
    disk/docker state
    """

    id: str  # noqa: A003
    backend: str
    type: TaskType  # noqa: A003
    definition: dict
    attributes: dict
    created_at: int = None

    @classmethod
    def from_task(cls, task):
        return cls(
            id=task.id,
            backend=task.backend,
            type=task.type,
            definition=task.definition,
            created_at=task.created_at,
            attributes=task.attributes,
        )

    def asdict(self):
        data = dataclasses.asdict(self)
        for key, value in data.items():
            # Convert Enums to strings for straightforward JSON serialization
            if isinstance(value, Enum):
                data[key] = value.value
        return data

    @classmethod
    def from_dict(cls, task_dict):
        task_type = TaskType.from_value(task_dict["type"])
        task_dict["type"] = task_type
        return cls(**task_dict)


@dataclass
class JobTaskResults:
    """Results of a RUNJOB or CANCELJOB task

    This represents the redacted version of the job metadata that is sent to the controller
    by the agent.
    """

    exit_code: int
    image_id: str
    message: str = None
    unmatched_hint: str = None
    # timestamp these results were finalized, in integer nanoseconds
    timestamp_ns: int = None

    # to be extracted from the image labels
    action_version: str = "unknown"
    action_revision: str = "unknown"
    action_created: str = "unknown"
    base_revision: str = "unknown"
    base_created: str = "unknown"

    has_unmatched_patterns: bool = (
        False  # this job was missing outputs that matched expected patterns
    )
    has_level4_excluded_files: bool = (
        False  # had files that were not copied to level 4 (too big or similar reason)
    )

    def to_dict(self):
        return dict(
            exit_code=self.exit_code,
            docker_image_id=self.image_id,
            status_message=self.message,
            hint=self.unmatched_hint,
            timestamp_ns=self.timestamp_ns,
            action_version=self.action_version,
            action_revision=self.action_revision,
            action_created=self.action_created,
            base_revision=self.base_revision,
            base_created=self.base_created,
            has_unmatched_patterns=self.has_unmatched_patterns,
            has_level4_excluded_files=self.has_level4_excluded_files,
        )

    @classmethod
    def from_dict(cls, metadata: dict):
        try:
            exit_code = int(metadata["exit_code"])
        except (TypeError, ValueError):
            exit_code = None
        return cls(
            exit_code=exit_code,
            image_id=metadata["docker_image_id"],
            message=metadata["status_message"],
            unmatched_hint=metadata["hint"],
            timestamp_ns=metadata["timestamp_ns"],
            action_version=metadata["action_version"],
            action_revision=metadata["action_revision"],
            action_created=metadata["action_created"],
            base_revision=metadata["base_revision"],
            base_created=metadata["base_created"],
            has_unmatched_patterns=metadata["has_unmatched_patterns"],
            has_level4_excluded_files=metadata["has_level4_excluded_files"],
        )
