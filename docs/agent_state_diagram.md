# Agent state diagram

This state diagram describes the state changes of Tasks from the
perspective of the Agent.

Note that the Agent is only concerned with executing Tasks. At each stage, task state is sent back to the controller so that the
Controller can update Job state.

The process differs depending on the type of task. The diagrams below show the flow for the two job-related tasks: `RUNJOB` and `CANCELJOB`.

```mermaid
---
title: RUNJOB Task
---
stateDiagram
    %% runjob
    unknown: Unknown
    prepared: Prepared
    executing: Executing
    executed: Executed
    finalized: Finalized
    error: Error

    [*] --> unknown: new RUNJOB task identified

    unknown --> prepared: prepare()

    prepared --> executing: execute()

    state execution_fail_fork_state <<choice>>
    executing --> execution_fail_fork_state
    execution_fail_fork_state --> executed: job runs to completion
    execution_fail_fork_state --> error: job errors

    error --> executed
    executed --> finalized: finalize()

    finalized --> [*]: cleanup()

    note right of unknown
        not started
        volume does not exist
        container does not exist
        Task stage UNKNOWN
    end note

    note right of prepared
        volume exists
        container does not exist
        Task stage PREPARED
    end note

    note left of executing
        volume exists
        container exists
        Task stage EXECUTING
    end note

    note left of executed
        volume exists
        container exists
        Task stage EXECUTED
    end note

    note left of finalized
        volume exists
        container exists
        Task stage FINALIZED
    end note

    note right of error
        volume exists
        container exists
        Task stage ERROR
    end note


classDef success fill:lightgreen;
classDef error fill:red;

class error error
class prepared,executing,executed,finalized success
```

The flow for a CANCELJOB task depends on the initial state of docker containers and volumes associated with the job to be cancelled.

```mermaid
---
title: CANCELJOB Task
---
stateDiagram
    %% runjob
    prepared: Prepared
    executing: Executing
    executed: Executed
    finalized: Finalized
    error: Error
    unknown: Unknown
    cleanup: cleanup()
    finalizing_states: finalize(cancelled=True)


    state initial_fork <<choice>>

    [*] --> initial_fork: new CANCELJOB task identified

    initial_fork --> unknown
    initial_fork --> prepared
    initial_fork --> executed
    initial_fork --> error
    initial_fork --> executing
    initial_fork --> finalized: nothing to do

    state finalizing_states <<join>>

    unknown --> finalizing_states
    prepared --> finalizing_states
    error --> finalizing_states
    executing --> finalizing_states: terminate()
    executed --> finalizing_states
    finalizing_states --> finalized

    finalized --> cleanup
    cleanup --> [*]

    note right of unknown
        not started
        volume does not exist
        container does not exist
        Task stage UNKNOWN
    end note

    note right of prepared
        volume exists
        container does not exist
        Task stage PREPARED
    end note

    note left of executing
        volume exists
        container exists
        Task stage EXECUTING
    end note

    note left of executed
        volume exists
        container exists
        Task stage EXECUTED
    end note

    note left of finalized
        volume exists
        container exists
        Task stage FINALIZED
    end note

    note right of error
        volume exists
        container exists
        Task stage ERROR
    end note

    note right of cleanup
        Delete container and volume for all
        states
        Note: nothing to do for UNKNOWN (job not started yet)
        and FINALIZED (job already done)
    end note


classDef success fill:lightgreen;
classDef error fill:red;
classDef action fill:#ebebeb,stroke:gray,stroke-width:0.5px,stroke-dasharray: 3 3;

class error error
class prepared,executing,executed,finalized success
class cleanup,finalizing_states action
```
