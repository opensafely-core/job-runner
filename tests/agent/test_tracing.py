import time
from unittest.mock import patch

from opentelemetry import trace

from jobrunner.agent import main, task_api
from jobrunner.agent.tracing import (
    set_job_span_metadata,
    set_task_span_metadata,
    trace_job_attributes,
    trace_job_results_attributes,
    trace_task_attributes,
)
from jobrunner.executors import local
from jobrunner.job_executor import ExecutorState
from jobrunner.tracing import OTEL_ATTR_TYPES
from tests.agent.stubs import StubExecutorAPI
from tests.conftest import get_trace
from tests.factories import job_definition_factory, runjob_db_task_factory


@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_tracing_state_change_attributes(mock_update_controller, db):
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
        "input_job_ids",
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


@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_tracing_final_state_attributes(mock_update_controller, db):
    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.EXECUTED)
    api.set_job_transition(
        job_id, ExecutorState.FINALIZED, hook=lambda job: api.set_job_metadata(job.id)
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
        "input_job_ids",
        "allow_database_access",
        "cpu_count",
        "memory_limit",
        "final_job_status",
        "complete",
        # results included on the final span
        "image_id",
        "executor_message",
        "exit_code",
        "action_version",
        "action_revision",
        "action_created",
        "base_revision",
        "base_created",
        "cancelled",
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
    # data about outputs or filename patterns is excluded
    for key in ["outputs", "unmatched_patterns", "unmatched_outputs"]:
        assert key not in spans[0].attributes


def test_set_task_span_metadata_no_attrs(db):
    task = runjob_db_task_factory()
    tracer = trace.get_tracer("test")

    span = tracer.start_span("test")
    set_task_span_metadata(span, task)
    assert span.attributes["backend"] == "test"
    assert span.attributes["task_type"] == "RUNJOB"
    assert span.attributes["task"] == task.id
    assert span.attributes["task_created_at"] == task.created_at * 1e9


def test_set_task_span_metadata_attrs(db):
    task = runjob_db_task_factory()
    tracer = trace.get_tracer("the-tracer")

    class CustomClass:
        def __str__(self):
            return "I am the custom class"

    span = tracer.start_span("the-span")
    set_task_span_metadata(
        span,
        task,
        custom_attr=CustomClass(),  # test that attr is added and the type coerced to string
        task_type="should be ignored",  # test that we can't override core task attributes
    )

    assert span.attributes["backend"] == "test"
    assert span.attributes["task_type"] == "RUNJOB"  # not "should be ignored"
    assert span.attributes["task"] == task.id
    assert span.attributes["task_created_at"] == task.created_at * 1e9
    assert span.attributes["custom_attr"] == "I am the custom class"


def test_task_trace_attributes_types(db):
    # check that all the trace attributes are of valid otel types
    # We test the attributes dict BEFORE it is set on the span (where it
    # will be turned into a string if it's an invalid type)
    task = runjob_db_task_factory()
    trace_attributes = trace_task_attributes(task)
    for value in trace_attributes.values():
        assert type(value) in OTEL_ATTR_TYPES


def test_set_job_span_metadata_no_attrs(db):
    job = job_definition_factory()
    tracer = trace.get_tracer("test")

    span = tracer.start_span("test")
    set_job_span_metadata(
        span,
        job,
    )
    assert span.attributes["job"] == job.id
    assert span.attributes["job_request"] == job.job_request_id
    assert span.attributes["workspace"] == job.workspace


def test_set_job_span_metadata_attrs(db):
    job = job_definition_factory()
    tracer = trace.get_tracer("the-tracer")

    class CustomClass:
        def __str__(self):
            return "I am the custom class"

    span = tracer.start_span("the-span")

    set_job_span_metadata(
        span,
        job,
        custom_attr=CustomClass(),  # test that attr is added and the type coerced to string
        action="should be ignored",  # test that we can't override core job attributes
    )

    assert span.attributes["job"] == job.id
    assert span.attributes["job_request"] == job.job_request_id
    assert span.attributes["workspace"] == job.workspace
    assert span.attributes["action"] == job.action  # not "should be ignored"
    assert span.attributes["custom_attr"] == "I am the custom class"


def test_set_job_span_metadata_tracing_errors_do_not_raise(db, caplog):
    job = job_definition_factory()
    tracer = trace.get_tracer("the-tracer")

    span = tracer.start_span("the-span")
    # mock Exception raised in function called by set_job_span_metadata
    with patch(
        "jobrunner.agent.tracing.trace_job_attributes", side_effect=Exception("foo")
    ):
        set_job_span_metadata(span, job)

    assert f"failed to trace job {job.id}" in caplog.text


def test_set_task_span_metadata_tracing_errors_do_not_raise(db, caplog):
    task = runjob_db_task_factory()
    tracer = trace.get_tracer("the-tracer")
    span = tracer.start_span("the-span")

    # mock Exception raised in function called by set_task_span_metadata
    with patch(
        "jobrunner.agent.tracing.trace_task_attributes", side_effect=Exception("foo")
    ):
        set_task_span_metadata(span, task)

    assert f"failed to trace task {task.id}" in caplog.text


@patch("jobrunner.agent.task_api.update_controller", spec=task_api.update_controller)
def test_tracing_final_state_attributes_tracing_errors(
    mock_update_controller, db, caplog
):
    api = StubExecutorAPI()

    task, job_id = api.add_test_runjob_task(ExecutorState.EXECUTED)
    api.set_job_transition(
        job_id, ExecutorState.FINALIZED, hook=lambda job: api.set_job_metadata(job.id)
    )
    with patch(
        "jobrunner.agent.tracing.set_span_attributes", side_effect=Exception("foo")
    ):
        main.handle_single_task(task, api)

    spans = get_trace("agent_loop")
    assert len(spans) == 1
    span = spans[0]
    # Exception encountered in set_span_attributes(), so only attributes set
    # directly with span.set_attributes are set, and nothing is raised
    assert span.attributes == {"initial_job_status": "EXECUTED"}
    assert "failed to trace job results" in caplog.text


def test_job_trace_attributes_has_expected_types(db):
    # check that all the trace attributes are of valid otel types
    # We test the attributes dict BEFORE it is set on the span (where it
    # will be turned into a string if it's an invalid type)
    job_definition = job_definition_factory()
    results_metadata = {
        "outputs": ["outputs"],
        "unmatched_patterns": [],
        "unmatched_outputs": [],
        "timestamp_ns": time.time_ns(),
        "status_message": "message",
        "hint": "hint",
    }

    results = local.get_job_metadata(
        job_definition=job_definition,
        container_metadata={"State": {"ExitCode": 0, "OOMKilled": False}},
        results_metadata=results_metadata,
    )

    trace_attributes = trace_job_results_attributes(
        results, trace_job_attributes(job_definition)
    )

    for key, value in trace_attributes.items():
        if value is None:
            continue
        assert type(value) in OTEL_ATTR_TYPES, key
