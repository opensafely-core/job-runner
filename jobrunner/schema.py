from dataclasses import dataclass
from enum import Enum


class TaskType(Enum):
    RUNJOB = "runjob"
    CANCELJOB = "canceljob"
    # TODO: delete job
    DBSTATUS = "dbstatus"


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
