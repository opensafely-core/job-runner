from jobrunner.lib import database  # cheating!
from jobrunner.models import Task as ControllerTask  # cheating!
from jobrunner.schema import AgentTask


def get_active_tasks() -> list[AgentTask]:
    """Get a list of active tasks from the controller"""
    # cheating for now - should be HTTP API call
    return [
        AgentTask(
            id=t.id,
            backend=t.backend,
            type=t.type,
            definition=t.definition,
            created_at=t.created_at,
        )
        for t in database.find_where(ControllerTask, active=True)
    ]


def update_controller(
    task: AgentTask,
    stage: str,
    results: dict = None,
    complete: bool = False,
):
    """Update the controller with the current state of the task.

    stage: the current stage of the task from the agent's perspective
    results: optional dictionary of completed results of this task, expected to be immutable
    complete: if the agent considers this task complete
    """
    # cheating for now - should be HTTP API call, which ends up updating the
    # controller db, but we just update the db directly
    #
    # this currently just updates the task table, and lets the main controller
    # loop update the jobs table as needed, and handle a completed task. In the
    # future, we may want the HTTP handler to do both, so that the main loop
    # does not need to handle agent updates and completed jobs at all.  But all
    # we have currently is the loop, so we'll do that logic there for step 1
    db_task = database.find_one(ControllerTask, id=task.id)
    db_task.agent_stage = stage
    db_task.agent_results = results
    db_task.agent_complete = complete
    database.update(db_task)
