from jobrunner.agent import main
from jobrunner.job_executor import ExecutorState
from tests.agent.stubs import StubExecutorAPI
from tests.conftest import get_trace


def test_tracing_state_change_attributes(db):
    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.UNKNOWN)
    # prepare is synchronous
    api.set_job_transition(job_id, ExecutorState.PREPARED)
    main.handle_single_task(task, api)

    spans = get_trace("agent_loop")
    # one span each time we called main.handle_single_task
    assert len(spans) == 1
    span = spans[0]

    # check we have the keys we expect
    assert set(span.attributes.keys()) == {
        "backend",
        "task_type",
        "task",
        "task_created_at",
        "initial_job_status",
        "job",
        "job_request",
        "workspace",
        "repo_url",
        "commit",
        "action",
        "job_created_at",
        "image",
        "args",
        "inputs",
        "allow_database_access",
        "cpu_count",
        "memory_limit",
        "final_job_status",
        "complete",
    }
    # attributes added from the task
    assert span.attributes["backend"] == "test"
    assert span.attributes["task_type"] == "RUNJOB"
    assert span.attributes["task"] == task.id
    assert span.attributes["task_created_at"] == task.created_at * 1e9
    # attributes added from the job
    assert span.attributes["job"] == job_id
    assert spans[0].attributes["initial_job_status"] == "UNKNOWN"
    assert spans[0].attributes["final_job_status"] == "PREPARED"
    assert not spans[0].attributes["complete"]


def test_tracing_final_state_attributes(db):
    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.EXECUTED)
    api.set_job_transition(
        job_id, ExecutorState.FINALIZED, hook=lambda job_id: api.set_job_result(job_id)
    )
    main.handle_single_task(task, api)

    spans = get_trace("agent_loop")
    # one span each time we called main.handle_single_task
    assert len(spans) == 1
    span = spans[0]

    # check we have the keys we expect
    assert set(span.attributes.keys()) == {
        "backend",
        "task_type",
        "task",
        "task_created_at",
        "initial_job_status",
        "job",
        "job_request",
        "workspace",
        "repo_url",
        "commit",
        "action",
        "job_created_at",
        "image",
        "args",
        "inputs",
        "allow_database_access",
        "cpu_count",
        "memory_limit",
        "final_job_status",
        "complete",
        # results included on the final span
        "unmatched_patterns",
        "image_id",
        "unmatched_outputs",
        "message",
        "exit_code",
    }
    # attributes added from the task
    assert span.attributes["backend"] == "test"
    assert span.attributes["task_type"] == "RUNJOB"
    assert span.attributes["task"] == task.id
    assert span.attributes["task_created_at"] == task.created_at * 1e9
    # attributes added from the job
    assert span.attributes["job"] == job_id
    assert spans[0].attributes["initial_job_status"] == "EXECUTED"
    assert spans[0].attributes["final_job_status"] == "FINALIZED"
    assert spans[0].attributes["complete"]
