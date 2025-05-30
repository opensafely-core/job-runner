# Controller state diagram

This state diagram describes the state changes of Jobs from the
perspective of the Controller.

States in orange are pending (i.e. prior to a task being picked
up by the Agent). States in red are final failed states, and those in green are final success states.

Note that during the Agent-handled stages, task state is posted back to the controller in order to update Job state. The Agent does not itself update Job state.

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
    reset_reboot_dbmaintenance: Reset
    job_error: Retry
    controller_error: Retry
    waiting: Waiting (Various)
    cancelled: Cancelled
    failed: Job Failed
    dependency_failed: Dependency Failed
    initiated_join: RUNJOB task created
    non_fatal_error: Non-fatal error

     [*] --> created: job created
    created --> cancelled: RUNJOB task deactivated, CANCELJOB task created
    created --> waiting
    created --> dependency_failed
    created --> reset_reboot_dbmaintenance

    state initiated_join <<join>>
    waiting --> initiated_join
    created --> initiated_join
    reset_reboot_dbmaintenance --> initiated_join
    initiated_join --> initiated


    initiated --> AgentStages
    AgentStages: Agent
    state AgentStages {
        prepared --> executing

    state execution_fail_fork_state <<choice>>
    executing --> execution_fail_fork_state
    execution_fail_fork_state --> executed: job runs to completion
    execution_fail_fork_state --> error: job errors

    error --> executed
    executed --> finalized

    }

    state finalized_fork_state <<choice>>
    AgentStages --> finalized_fork_state
    finalized_fork_state --> succeeded
    finalized_fork_state --> failed: fatal error
    finalized_fork_state --> non_fatal_error


    job_error --> created
    controller_error --> initiated

    non_fatal_error --> job_error: job error
    non_fatal_error --> controller_error: controller error

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

    note left of waiting
        job.state PENDING
    end note

    note right of reset_reboot_dbmaintenance
        Existing RUNJOB task cancelled due to reboot or DB maintenance
        Job reset waiting for retry task
        job.state PENDING
        job.status_code WAITING_ON_REBOOT / WAITING_DBMAINTENANCE
    end note

    note right of succeeded
        job.state SUCCEEDED
        job.status_code SUCCEEDED
    end note

    note right of cancelled
        job.state FAILED
        job.status_code CANCELLED_BY_USER
    end note

    note right of failed
        job.state FAILED
        job.status_code Various cancelled/failed states determined by Controller, including
            CANCELLED_BY_USER
            JOB_ERROR
            UNMATCHED_PATTERNS
            NONZERO_EXIT
    end note

    note left of job_error
        Previous RUNJOB task encountered a non-fatal error (reported by agent)
        job.state PENDING
        job.status_code WAITING_ON_NEW_TASK
    end note

    note left of controller_error
        Controller encountered a non-fatal error
        Controller restarts
        Existing RUNJOB task remains active
        Job state is unchanged
    end note

classDef success fill:lightgreen;
classDef pending fill:orange;
classDef error fill:red;
classDef action fill:#ebebeb,stroke:gray,stroke-width:0.5px,stroke-dasharray: 3 3;

class created,initiated,succeeded,waiting,retry,reset_reboot_dbmaintenance,job_error,controller_error pending
class failed,cancelled,dependency_failed error
class succeeded success
class initiated_join,non_fatal_error action
```
