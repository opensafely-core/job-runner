from dataclasses import dataclass
from enum import Enum


class TaskType(Enum):
    RUNJOB = "runjob"
    CANCELJOB = "canceljob"
    # TODO: delete job


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
