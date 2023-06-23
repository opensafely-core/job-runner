# Local Executor state diagram

This state diagram describes the amalgamation of `local_executor.py` and `run.py` to facilitate understanding changes that cut across both of them, this was really useful for @madwort when working on the intricacies of cancellation. Details should be verified with the current version of the code, and also cross-referenced with StubExecutor & the pytests.

```mermaid
stateDiagram
    state1: Pending
    state2: Prepared
    state3: Executing
    state4: Executed
    state5: Finalized
    state6: Job Succeeded
    state7: Error
    state8: Cancelled (Prepared)
    state9: Cancelled (Executing)
    state10: Job Failed
    state11: Cancelled (Pending)
    state12: Waiting (Various)
    state13: Finalized (cancelled prepared)

    note right of state2
        volume exists
        container does not exist
        job.state RUNNING
        job.status_code (should be) PREPARED
        status.ExecutorState PREPARED
        get_status().ExecutorState PREPARED
        cancelled falsey
    end note

    note left of state3
        volume exists
        container exists
        job.state RUNNING
        job.status_code EXECUTING
        status.ExecutorState EXECUTING
        get_status().ExecutorState EXECUTING
        cancelled falsey
    end note

    note left of state4
        volume exists
        container exists
        job.state RUNNING
        job.status_code EXECUTED
        status.ExecutorState N/A
        get_status().ExecutorState EXECUTED
        cancelled falsey
    end note

    note left of state5
        volume exists
        container exists
        job.state RUNNING
        job.status_code FINALIZED
        status.ExecutorState FINALIZED
        get_status().ExecutorState FINALIZED
    end note

    note right of state6
        volume does not exist
        container does not exist
        job.state SUCCEEDED
        job.status_code SUCCEEDED
        status.ExecutorState UNKNOWN?
        get_status().ExecutorState UNKNOWN?
        cancelled falsey
    end note

    note right of state7
        volume exists
        container exists
        job.state RUNNING
        job.status_code EXECUTED
        status.ExecutorState N/A
        get_status().ExecutorState EXECUTED
        cancelled falsey
    end note

   note right of state8
        volume exists
        container does not exist
        job.state RUNNING
        job.status_code PREPARED
        status.ExecutorState N/A
        get_status().ExecutorState PREPARED
        cancelled truthy
    end note

    note right of state9
        volume exists
        container exists
        job.state RUNNING
        job.status_code EXECUTING
        status.ExecutorState EXECUTING
        get_status().ExecutorState EXECUTING
        cancelled truthy
    end note

    note right of state10
        volume does not exist
        container does not exist
        job.state FAILED
        job.status_code CANCELLED_BY_USER
        status.ExecutorState UNKNOWN
        get_status().ExecutorState UNKNOWN
        cancelled truthy
    end note

    note right of state11
        volume does not exist
        container does not exists
        job.state FAILED
        job.status_code CANCELLED_BY_USER
        status.ExecutorState UNKNOWN
        get_status().ExecutorState UNKNOWN
        cancelled truthy
    end note

    note right of state13
        volume exists
        container does not exists
        job.state RUNNING
        job.status_code FINALIZED
        status.ExecutorState N/A
        get_status().ExecutorState FINALIZED
        cancelled truthy
    end note
    [*] --> state1: job created
    state1 --> state12
    state12 --> state1

    state1 --> state2: prepare()

    state2 --> state3: execute()
    state2 --> state8: set cancelled


    state execution_fail_fork_state <<choice>>
    state3 --> execution_fail_fork_state
    execution_fail_fork_state --> state4: job runs to completion
    execution_fail_fork_state --> state7: job errors
    state3 --> state9: set cancelled & run loop

    state7 --> state4
    state8 --> state13: job loop
    state9 --> state4: terminate()
    state4 --> state5: finalize()

    state pre_cleanup <<join>>
    state13 --> pre_cleanup
    state5 --> pre_cleanup

    state post_cleanup_fork <<choice>>
    pre_cleanup --> post_cleanup_fork: cleanup()
    post_cleanup_fork --> state6: cancelled is falsey
    post_cleanup_fork --> state10: cancelled is truthy

    state1 --> state11: set cancelled & run loop

    state6 --> [*]
    state10 --> [*]
    state11 --> [*]

```
