import dataclasses
from dataclasses import dataclass
from enum import Enum


class TaskType(Enum):
    RUNJOB = "runjob"
    CANCELJOB = "canceljob"
    # TODO: delete job
    DBSTATUS = "dbstatus"

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
    created_at: int = None

    @classmethod
    def from_task(cls, task):
        return cls(
            id=task.id,
            backend=task.backend,
            type=task.type,
            definition=task.definition,
            created_at=task.created_at,
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
