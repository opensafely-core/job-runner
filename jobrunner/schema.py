from dataclasses import dataclass
from enum import Enum

from jobrunner.job_executor import ExecutorState


class TaskType(Enum):
    RUNJOB = "runjob"
    # TODO: delete job


class TaskStage(Enum):
    """Valid stage of any tasks

    A big merge of multiple types of TaskType's stages
    """

    # RUNJOB stages
    PREPARING = ExecutorState.PREPARING.value
    PREPARED = ExecutorState.PREPARED.value
    EXECUTING = ExecutorState.EXECUTING.value
    EXECUTED = ExecutorState.EXECUTED.value
    FINALIZING = ExecutorState.FINALIZING.value
    # Final stages
    FINALIZED = ExecutorState.FINALIZED.value
    ERROR = ExecutorState.ERROR.value


@dataclass(frozen=True)
class AgentTask:
    """Task API task data

    This is basically all the information about the task supplied by the
    controller, and is effectively immutable.

    The agent's view of a task's current state is computed on demand from
    disk/docker state
    """

    id: str  # noqa: A003
    type: TaskType  # noqa: A003
    definition: dict
    created_at: int = None
