# Job execution state diagram

This state diagram describes the amalgamation of `controller/main.py`, `agent/main.py` and `local_executor.py` to facilitate understanding changes that cut across both of them.

States in orange are handled by the Controller, and those in green are handled by the Agent and executor.

Note that during Agent-handled stages, task state is posted back to the controller in order to update Job state. The Agent does not itself update Job state.

```mermaid
stateDiagram
    created: Created
    initiated: Initiated
    prepared: Prepared
    executing: Executing
    executed: Executed
    finalized: Finalized
    succeeded: Job Succeeded
    error: Error
    cancelled_prepared: Cancelled (Prepared)
    cancelled_executing: Cancelled (Executing)
    failed: Job Failed
    cancelled_pending: Cancelled (Pending)
    dependency_failed: Dependency Failed
    waiting: Waiting (Various)
    finalized_cancelled_prepared: Finalized (cancelled prepared)

     [*] --> created: job created
    created --> initiated: RUNJOB task created
    created --> cancelled_prepared: RUNJOB task deactivated, CANCELJOB task created
    created --> cancelled_executing: RUNJOB task deactivated, CANCELJOB task created
    created --> cancelled_pending
    created --> waiting
    waiting --> initiated: RUNJOB task created
    created --> dependency_failed

    initiated --> prepared: prepare()

    prepared --> executing: execute()

    state execution_fail_fork_state <<choice>>
    executing --> execution_fail_fork_state
    execution_fail_fork_state --> executed: job runs to completion
    execution_fail_fork_state --> error: job errors

    error --> executed
    cancelled_prepared --> finalized_cancelled_prepared: finalize()
    cancelled_executing --> executed: terminate()
    executed --> finalized: finalize()

    finalized_cancelled_prepared --> clean: cleanup()
    finalized --> clean: cleanup()

    state clean <<choice>>
    clean --> succeeded
    clean --> failed: cancelled or error

    note right of initiated
        Task created, ready to be picked up by Agent
        job.state RUNNING
        job.status_code INITIATED
    end note

    note right of dependency_failed
        Failed due to a dependency failure.
        No task created.
        job.state FAILED
        job.status_code DEPENDENCY_FAILED
    end note

    note right of waiting
        job.state PENDING
        Various possible pending job status_codes:
        WAITING_ON_DEPENDENCIES
        WAITING_PAUSED
        WAITING_DB_MAINTENANCE
        WAITING_ON_DEPENDENCIES
        WAITING_ON_WORKERS
        WAITING_ON_DB_WORKERS
        WAITING_ON_REBOOT
        WAITING_ON_NEW_TASK
    end note

    note right of prepared
        volume exists
        container does not exist
        job.state RUNNING
        job.status_code PREPARED
        get_status() PREPARED
    end note

    note left of executing
        volume exists
        container exists
        job.state RUNNING
        job.status_code EXECUTING
        get_status() EXECUTING
    end note

    note left of executed
        volume exists
        container exists
        job.state RUNNING
        job.status_code EXECUTED
        get_status() EXECUTED
    end note

    note left of finalized
        volume exists
        container exists
        job.state RUNNING
        job.status_code FINALIZED
        get_status() FINALIZED
    end note

    note right of succeeded
        volume does not exist
        container does not exist
        job.state SUCCEEDED
        job.status_code SUCCEEDED
        get_status() FINALIZED
    end note

    note right of error
        volume exists
        container exists
        job.state RUNNING
        job.status_code EXECUTED
        get_status() EXECUTED
    end note

   note right of cancelled_prepared
        volume exists
        container does not exist
        job.state RUNNING
        job.status_code PREPARED
        get_status() PREPARED
    end note

    note right of cancelled_executing
        volume exists
        container exists
        job.state RUNNING
        job.status_code EXECUTING
        get_status() EXECUTING
    end note

    note right of failed
        volume does not exist
        container does not exist
        job.state FAILED
        job.status_code Various canelled/failed states, including
            CANCELLED_BY_USER
            JOB_ERROR
            UNMATCHED_PATTERNS
            NONZERO_EXIT
        get_status() ExecutorState FINALIZED / ERROR
    end note

    note left of cancelled_pending
        RUNJOB task not created, no CANCELJOB task required
        job.state FAILED
        job.status_code CANCELLED_BY_USER
    end note

    note right of clean
        volume does not exist
        container does not exist
        job metadata written to persist current state
    end note

classDef agent fill:lightgreen;
classDef controller fill:orange;

class created,initiated,succeeded,failed,cancelled_pending,waiting,dependency_failed controller
class prepared,executing,executed,finalized,error,cancelled_prepared,cancelled_executing,finalized_cancelled_prepared agent
```
