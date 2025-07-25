import datetime
import logging
import re
import sqlite3
from unittest.mock import Mock, patch

import pytest

import controller
from agent import config as agent_config
from agent import main as agent_main
from common import config as common_config
from controller import config, main, task_api
from controller.lib import database
from controller.models import Job, State, StatusCode, Task, TaskType
from controller.queries import get_flag_value, set_flag
from tests.conftest import get_trace
from tests.factories import (
    job_factory,
    job_request_factory,
    job_task_results_factory,
    runjob_db_task_factory,
)


def run_controller_loop_once():
    main.main(exit_callback=lambda _: True)


def run_agent_loop_once():
    agent_main.main(exit_callback=lambda _: True)


def set_job_task_results(job, task_results, error=None):
    runjob_task = database.find_one(
        Task, type=TaskType.RUNJOB, id__glob=f"{job.id}-*", active=True
    )
    results = task_results.to_dict()
    results["error"] = error or False

    task_api.handle_task_update(
        task_id=runjob_task.id,
        stage="",
        results=results,
        complete=True,
    )


def test_handle_pending_job_with_previous_tasks(db):
    # Make a runjob task for a pending job
    # (This is an error; if a job is pending, it should have
    # no active runjob tasks)
    job = job_factory(state=State.PENDING)
    task = runjob_db_task_factory(job)

    tasks = database.find_where(Task, type=TaskType.RUNJOB)
    assert len(tasks) == 1
    assert tasks[0].active

    with pytest.raises(AssertionError):
        run_controller_loop_once()

    # Make task inactive and run the controller loop again
    task_api.mark_task_inactive(task)

    run_controller_loop_once()

    # Controller has created a new runjob task
    tasks = database.find_where(Task, type=TaskType.RUNJOB)
    assert len(tasks) == 2
    assert not tasks[0].active
    assert tasks[1].active
    job = database.find_one(Job, id=job.id)
    assert job.state == State.RUNNING


def test_handle_pending_job_cancelled(db):
    job = job_factory()
    run_controller_loop_once()

    tasks = database.find_where(Task, type=TaskType.RUNJOB)
    assert len(tasks) == 1
    assert tasks[0].active

    database.update_where(Job, dict(cancelled=True), id=job.id)

    run_controller_loop_once()

    runjob_tasks = database.find_where(Task, type=TaskType.RUNJOB)
    canceljob_tasks = database.find_where(Task, type=TaskType.CANCELJOB)
    assert len(runjob_tasks) == 1
    assert not runjob_tasks[0].active
    assert len(canceljob_tasks) == 1

    job = database.find_one(Job, id=job.id)

    assert job.state == State.FAILED
    assert job.status_message == "Cancelled by user"
    assert job.status_code == StatusCode.CANCELLED_BY_USER


def test_handle_job_pending_dependency_failed(db):
    dependency = job_factory(state=State.FAILED)
    job = job_factory(
        state=State.PENDING,
        job_request_id=dependency.job_request_id,
        action="action2",
        wait_for_job_ids=[dependency.id],
    )

    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "Not starting as dependency failed"
    assert job.status_code == StatusCode.DEPENDENCY_FAILED

    # tracing
    spans = get_trace("jobs")
    assert all(s.attributes["job.backend"] == "test" for s in spans)
    assert spans[-3].name == "CREATED"
    assert spans[-2].name == "DEPENDENCY_FAILED"
    assert spans[-2].status.is_ok
    assert spans[-1].name == "JOB"
    assert spans[-1].status.is_ok


def test_handle_pending_job_waiting_on_dependency(db):
    dependency = job_factory()
    job = job_factory(
        state=State.PENDING,
        job_request_id=dependency.job_request_id,
        action="action2",
        wait_for_job_ids=[dependency.id],
    )

    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    # our state
    assert job.state == State.PENDING
    assert job.status_message == "Waiting on dependencies"
    assert job.status_code == StatusCode.WAITING_ON_DEPENDENCIES

    # tracing
    spans = get_trace("jobs")
    assert all(s.attributes["job.backend"] == "test" for s in spans)
    assert spans[-1].name == "CREATED"


def test_handle_job_waiting_on_workers(monkeypatch, db):
    monkeypatch.setattr(config, "MAX_WORKERS", {"test": 0})

    job = job_factory()
    run_controller_loop_once()

    tasks = database.find_where(Task, type=TaskType.RUNJOB)
    job = database.find_one(Job, id=job.id)

    assert len(tasks) == 0
    assert job.state == State.PENDING
    assert job.status_message == "Waiting on available workers"
    assert job.status_code == StatusCode.WAITING_ON_WORKERS

    # tracing
    spans = get_trace("jobs")
    assert all(s.attributes["job.backend"] == "test" for s in spans)
    assert spans[-1].name == "CREATED"


def test_handle_job_waiting_on_workers_by_backend(monkeypatch, db):
    # backends can run at most 1 job
    monkeypatch.setattr(config, "MAX_WORKERS", {"foo": 1, "bar": 1})
    monkeypatch.setattr(config, "DEFAULT_JOB_CPU_COUNT", {"foo": 2, "bar": 2})
    monkeypatch.setattr(config, "DEFAULT_JOB_MEMORY_LIMIT", {"foo": "4G", "bar": "4G"})

    # One running job on backend foo
    # No running jobs on backend bar
    running_job = job_factory(backend="foo")
    # run loop once to set it running
    run_controller_loop_once()
    tasks = database.find_where(Task, type=TaskType.RUNJOB)
    assert len(tasks) == 1
    running_job = database.find_one(Job, id=running_job.id)
    assert running_job.state == State.RUNNING

    pending_job1 = job_factory(backend="foo")
    pending_job2 = job_factory(backend="bar")
    run_controller_loop_once()

    tasks = database.find_where(Task, type=TaskType.RUNJOB)
    # Only one task could be created, for the pending job on backend bar
    assert len(tasks) == 2
    assert tasks[-1].id.startswith(pending_job2.id)
    pending_job1 = database.find_one(Job, id=pending_job1.id)
    assert pending_job1.state == State.PENDING
    assert pending_job1.status_message == "Waiting on available workers"
    assert pending_job1.status_code == StatusCode.WAITING_ON_WORKERS

    pending_job2 = database.find_one(Job, id=pending_job2.id)
    assert pending_job2.state == State.RUNNING

    # tracing
    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"
    assert spans[-1].attributes["job.backend"] == "foo"
    assert spans[-2].attributes["job.backend"] == "bar"


def test_handle_job_waiting_on_workers_resource_intensive_job(monkeypatch, db):
    monkeypatch.setattr(config, "MAX_WORKERS", {"test": 2})
    monkeypatch.setattr(
        config,
        "JOB_RESOURCE_WEIGHTS",
        {"test": {"workspace": {re.compile(r"action\d{1}"): 1.5}}},
    )

    # Resource-heavy jobs can be configured with a weighting, which is used as a
    # multiplier to determine how many resources are needed. This means that we don't
    # start a resource-heavy job unless there are extra workers available.

    # Used resources are calculated by summing the currently running actions multiplied by
    # their weights (or a default 1)
    # Required resources are calculated similarly for the current job.
    # A job can start if the used resources + required resources are less than our MAX_WORKERS

    # This action requires 1.5 resources. No other jobs are running, so we have 2 resources (i.e.
    # the MAX_WORKERS) currently available.
    # We set requires_db to ensure it's the first one run
    job1 = job_factory(workspace="workspace", action="action1", requires_db=True)

    # This action requires 1.5 resources. job1 is already running and using 1.5 resources.
    # We have 2 max workers, so only 0.5 resources are left after first one is running
    job2 = job_factory(workspace="workspace", action="action2")

    # This action requires 1 resource, so will only run when at least 1 resource is available
    # Only 0.5 resources are left as job1 is using 1.5 is running
    job3 = job_factory(workspace="workspace", action="non_matching_action")
    run_controller_loop_once()

    job1 = database.find_one(Job, id=job1.id)
    job2 = database.find_one(Job, id=job2.id)
    job3 = database.find_one(Job, id=job3.id)

    assert job1.state == State.RUNNING
    assert job1.status_code == StatusCode.INITIATED

    assert job2.state == State.PENDING
    assert (
        job2.status_message == "Waiting on available workers for resource intensive job"
    )
    assert job2.status_code == StatusCode.WAITING_ON_WORKERS

    assert job3.state == State.PENDING
    assert job3.status_message == "Waiting on available workers"
    assert job3.status_code == StatusCode.WAITING_ON_WORKERS


def test_handle_job_waiting_on_db_workers(monkeypatch, db):
    monkeypatch.setattr(config, "MAX_DB_WORKERS", {"test": 0})
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
    )
    run_controller_loop_once()

    tasks = database.find_where(Task, type=TaskType.RUNJOB)
    job = database.find_one(Job, id=job.id)

    assert len(tasks) == 0
    assert job.state == State.PENDING
    assert job.status_message == "Waiting on available database workers"
    assert job.status_code == StatusCode.WAITING_ON_DB_WORKERS

    # tracing
    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_job_finalized_success_with_large_file(db):
    # insert previous outputs
    job_factory(
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        outputs={"output/output.csv": "moderately_sensitive"},
    )
    # create new job
    job = job_factory()

    run_controller_loop_once()
    set_job_task_results(
        job,
        job_task_results_factory(
            has_level4_excluded_files=True,
        ),
    )
    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    # our state
    assert job.state == State.SUCCEEDED
    assert "Completed successfully" in job.status_message
    assert "were excluded" in job.status_message
    assert "output/output.csv: too big" not in job.status_message


@pytest.mark.parametrize(
    "exit_code,run_command,extra_message,results_message",
    [
        (
            3,
            "ehrql generate-dataset dataset.py --output data.csv",
            (
                "A transient database error occurred, your job may run "
                "if you try it again, if it keeps failing then contact tech support"
            ),
            None,
        ),
        (
            4,
            "ehrql generate-dataset dataset.py --output data.csv",
            "New data is being imported into the database, please try again in a few hours",
            None,
        ),
        (
            5,
            "ehrql generate-dataset dataset.py --output data.csv",
            "Something went wrong with the database, please contact tech support",
            None,
        ),
        (
            5,
            "ehrql generate-dataset dataset.py --output data.csv",
            "Something went wrong with the database, please contact tech support",
            "A message from the results",
        ),
        (
            7,  # an unknown DATABASE_EXIT_CODE
            "ehrql generate-dataset dataset.py --output data.csv",
            None,
            None,
        ),
        # the same exit codes for a job that doesn't have access to the database show no message
        (3, "python foo.py", None, "A message from the results"),
        (4, "python foo.py", None, None),
        (5, "python foo.py", None, None),
    ],
)
def test_handle_job_finalized_failed_exit_code(
    exit_code, run_command, extra_message, results_message, db, backend_db_config
):
    job = job_factory(
        run_command=run_command,
        requires_db="ehrql" in run_command,
    )

    run_controller_loop_once()
    set_job_task_results(
        job,
        job_task_results_factory(
            exit_code=exit_code,
            message=results_message,
        ),
    )
    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    # our state
    assert job.state == State.FAILED
    assert job.status_code == StatusCode.NONZERO_EXIT

    expected = "Job exited with an error"

    # A message from the results beats a DB exit code message
    if results_message:
        expected += f": {results_message}"
    elif extra_message:
        expected += f": {extra_message}"
    assert job.status_message == expected
    assert job.outputs is None
    assert job.unmatched_outputs is None
    assert job.level4_excluded_files is None

    spans = get_trace("jobs")
    completed_span = spans[-2]
    assert completed_span.name == "NONZERO_EXIT"
    assert completed_span.attributes["job.exit_code"] == exit_code
    assert completed_span.attributes["job.image_id"] == "image_id"
    # data about outputs or filename patterns is excluded
    for key in ["job.outputs", "job.unmatched_patterns", "job.unmatched_outputs"]:
        assert key not in completed_span.attributes
    assert completed_span.status.is_ok
    assert spans[-1].name == "JOB"


def test_handle_job_finalized_failed_unmatched_patterns(db):
    job = job_factory()
    run_controller_loop_once()
    set_job_task_results(
        job,
        job_task_results_factory(has_unmatched_patterns=True),
    )
    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    # our state
    assert job.state == State.FAILED
    assert (
        job.status_message
        == "Outputs matching expected patterns were not found. See job log for details."
    )
    assert job.outputs is None
    assert job.unmatched_outputs is None

    spans = get_trace("jobs")
    completed_span = spans[-2]
    assert completed_span.name == "UNMATCHED_PATTERNS"
    # data about outputs or filename patterns is excluded
    for key in ["outputs", "unmatched_patterns", "unmatched_outputs"]:
        assert key not in completed_span.attributes
    assert spans[-1].name == "JOB"


def test_handle_job_finalized_failed_with_fatal_error(db):
    # insert previous outputs
    # create new job
    job = job_factory()

    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.RUNNING

    set_job_task_results(
        job, job_task_results_factory(), error=str(Exception("test_job_failure"))
    )

    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    # our state
    assert job.state == State.FAILED
    assert job.status_code == StatusCode.JOB_ERROR
    assert "fatal" in job.status_message

    spans = get_trace("loop")

    span = spans[-2]  # final span is loop job
    assert span.name == "LOOP_JOB"
    assert len(span.events) == 0

    assert span.status.is_ok


def test_handle_job_finalized_failed_with_non_fatal_error(db):
    # insert previous outputs
    # create new job
    job = job_factory()

    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.RUNNING

    set_job_task_results(
        job, job_task_results_factory(), error=str(Exception("test_soft_failure"))
    )

    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    tasks = database.find_where(Task, type=TaskType.RUNJOB)
    assert len(tasks) == 1
    assert not tasks[0].active

    # our state
    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_ON_NEW_TASK
    assert "retried" in job.status_message

    run_controller_loop_once()

    # Controller has created a new runjob task
    tasks = database.find_where(Task, type=TaskType.RUNJOB)
    assert len(tasks) == 2
    assert not tasks[0].active
    assert tasks[1].active

    job = database.find_one(Job, id=job.id)
    assert job.state == State.RUNNING

    spans = get_trace("loop")
    span = spans[-2]  # final span is loop job
    assert span.name == "LOOP_JOB"
    assert len(span.events) == 0


@pytest.fixture
def backend_db_config(monkeypatch):
    # for test jobs, job.database_name is None, so add a dummy connection
    # string for that db
    monkeypatch.setitem(agent_config.DATABASE_URLS, None, "conn str")


def test_handle_pending_db_maintenance_mode(db, backend_db_config):
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
    )
    set_flag("mode", "db-maintenance", job.backend)

    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)

    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_DB_MAINTENANCE
    assert job.status_message == "Waiting for database to finish maintenance"
    assert job.started_at is None

    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_pending_cancelled_db_maintenance_mode(db, backend_db_config):
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
        cancelled=True,
    )
    set_flag("mode", "db-maintenance", job.backend)

    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)

    assert job.state == State.FAILED
    assert job.status_code == StatusCode.CANCELLED_BY_USER
    assert job.status_message == "Cancelled by user"
    assert job.started_at is None


def test_handle_running_db_maintenance_mode(db, backend_db_config):
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
    )
    # Start it running, then set the flag
    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.RUNNING

    set_flag("mode", "db-maintenance", job.backend)
    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)

    # job has been set back to pending
    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_DB_MAINTENANCE
    assert job.status_message == "Waiting for database to finish maintenance"
    assert job.started_at is None

    # the RUNJOB task is no longer active and a new CANCELJOB task has been created
    tasks = database.find_where(Task, type__in=[TaskType.RUNJOB, TaskType.CANCELJOB])
    assert len(tasks) == 2
    assert tasks[0].type == TaskType.RUNJOB
    assert not tasks[0].active
    assert tasks[1].type == TaskType.CANCELJOB
    assert tasks[1].active


def test_handle_pending_pause_mode(db, backend_db_config, freezer):
    mock_now = datetime.datetime(2025, 3, 1, 10, 5)
    freezer.move_to(mock_now)
    mock_now_s = int(mock_now.timestamp())

    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
        status_code=StatusCode.CREATED,
    )
    reset_job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
        status_code=StatusCode.WAITING_ON_REBOOT,
    )

    assert job.updated_at == mock_now_s
    assert reset_job.updated_at == mock_now_s

    mock_later = datetime.datetime(2025, 3, 1, 10, 10)
    freezer.move_to(mock_later)
    mock_later_s = int(mock_later.timestamp())

    set_flag("paused", "True", job.backend)

    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)
    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_PAUSED
    assert job.started_at is None
    assert "paused" in job.status_message
    assert job.updated_at == mock_later_s

    # a reset job keeps its existing status code
    reset_job = database.find_one(Job, id=reset_job.id)
    assert reset_job.state == State.PENDING
    assert reset_job.status_code == StatusCode.WAITING_ON_REBOOT
    assert reset_job.started_at is None
    assert reset_job.updated_at == mock_later_s

    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_running_pause_mode(db, backend_db_config):
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
    )

    # Start it running, then pause, then update its status
    run_controller_loop_once()
    set_flag("paused", "True", job.backend)
    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    assert job.state == State.RUNNING
    assert "paused" not in job.status_message


def test_ignores_cancelled_jobs_when_calculating_dependencies(db):
    job_factory(
        id="1",
        action="other-action",
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        created_at=1000,
        outputs={"output-from-completed-run": "highly_sensitive_output"},
    )
    job_factory(
        id="2",
        action="other-action",
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        created_at=2000,
        cancelled=True,
        outputs={"output-from-cancelled-run": "highly_sensitive_output"},
    )
    job_factory(
        id="3",
        action="action-with-no-outputs",
        state=State.SUCCEEDED,
        status_code=StatusCode.SUCCEEDED,
        created_at=3000,
        outputs=None,
    )
    job_factory(
        id="4",
        requires_outputs_from=["other-action", "action-with-no-outputs"],
    )
    run_controller_loop_once()

    task = database.find_one(Task, type=TaskType.RUNJOB)
    # The task definition's inputs contains only the ids of the latest
    # uncancelled jobs that ran the action dependencies ("other-action",
    # "action-with-no-outputs")
    # Note that since we expect to not hold output filenames on the
    # controller, we still send the agent the id of the job with no
    # outputs
    assert task.definition["input_job_ids"] == ["1", "3"]


def test_job_definition_limits(db):
    job = job_factory()
    job_definition = main.job_to_job_definition(job, task_id="")
    assert job_definition.cpu_count == 2
    assert job_definition.memory_limit == "4G"


def datetime_to_ns(datetime):
    return datetime.timestamp() * 1e9


@pytest.mark.parametrize(
    "status_code_updated_at,new_status_code_updated_at",
    [
        (
            # previous updated at is before now
            datetime_to_ns(datetime.datetime(2025, 3, 1, 9, 5, 10, 99999)),
            # new updated_at is now (in ns)
            datetime_to_ns(datetime.datetime(2025, 3, 1, 10, 5, 10, 99999)),
        ),
        (
            # previous updated at is after now
            datetime_to_ns(datetime.datetime(2025, 3, 1, 10, 5, 11, 99999)),
            # new updated at timestamp is limited to 1ms after the previous one
            datetime_to_ns(datetime.datetime(2025, 3, 1, 10, 5, 11, 99999)) + 1e6,
        ),
    ],
)
def test_status_code_timing(
    db, freezer, status_code_updated_at, new_status_code_updated_at
):
    mock_now = datetime.datetime(2025, 3, 1, 10, 5, 10, 99999)
    freezer.move_to(mock_now)

    job = job_factory(
        state=State.PENDING,
        status_code=StatusCode.WAITING_ON_WORKERS,
        status_code_updated_at=status_code_updated_at,
    )
    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.RUNNING

    assert job.status_code_updated_at == new_status_code_updated_at


def test_status_code_unchanged_job_updated_at(db, freezer, caplog):
    mock_now = datetime.datetime(2025, 3, 1, 10, 5, 10, 99999)
    caplog.set_level(logging.INFO)
    freezer.move_to(mock_now)

    # setup a job that's waiting on a dependency; this will recall
    # set_code each time through the controller loop
    dependency = job_factory()
    job = job_factory(
        state=State.PENDING,
        job_request_id=dependency.job_request_id,
        action="action2",
        wait_for_job_ids=[dependency.id],
    )

    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)
    assert job.state == State.PENDING
    assert job.status_message == "Waiting on dependencies"
    assert job.status_code == StatusCode.WAITING_ON_DEPENDENCIES
    # updated at is set to the current timestamp in seconds
    assert job.updated_at == int(mock_now.timestamp())
    assert job.status_code_updated_at == datetime_to_ns(mock_now)

    # move forwards less than 1 min, updated_at does not change
    mock_now_1 = datetime.datetime(2025, 3, 1, 10, 5, 40, 99999)
    freezer.move_to(mock_now_1)
    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.PENDING
    assert job.updated_at == int(mock_now.timestamp())
    assert job.status_code_updated_at == datetime_to_ns(mock_now)

    # move forwards more than 1 min, updated_at is updated to current timestamp
    # status_code_updated_at does not change
    mock_now_2 = datetime.datetime(2025, 3, 1, 10, 6, 11, 99999)
    freezer.move_to(mock_now_2)
    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.PENDING
    assert job.updated_at == int(mock_now_2.timestamp())
    assert job.status_code_updated_at == datetime_to_ns(mock_now)

    last_info_log = [
        record for record in caplog.records if record.levelno == logging.INFO
    ][-1]
    assert last_info_log.message != "Waiting on dependencies"

    # For long running jobs, we log (at INFO level) that the job is still running
    # every 10 mins, calculated by checking if the current minute is divisible by
    # 10. This means we don't fill up the logs with "still running" messages on
    # every loop.
    # move forward to a time that's divisible by 10 mins
    mock_now_3 = datetime.datetime(2025, 3, 1, 10, 20, 11, 99999)
    freezer.move_to(mock_now_3)
    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.PENDING
    assert job.updated_at == int(mock_now_3.timestamp())
    assert job.status_code_updated_at == datetime_to_ns(mock_now)
    last_info_log = [
        record for record in caplog.records if record.levelno == logging.INFO
    ][-1]
    assert last_info_log.message == "Waiting on dependencies"


def test_status_code_updated_from_task_timestamp(db, freezer):
    mock_now = datetime.datetime(2025, 3, 1, 10, 5, 10, 99999)
    freezer.move_to(mock_now)

    job = job_factory(state=State.PENDING)
    # first run through the controller creates the task, changes the
    # job to running (initiated) and sets the status_code_updated_at to now
    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    task = main.get_task_for_job(job)
    assert task.agent_timestamp_ns is None
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.INITIATED
    assert job.updated_at == int(mock_now.timestamp())
    assert job.status_code_updated_at == datetime_to_ns(mock_now)

    # update task to PREPARED, with timestamp one second later
    prepared_at = datetime.datetime(2025, 3, 1, 10, 5, 11, 99999)
    task.agent_stage = StatusCode.PREPARED.value
    task.agent_timestamp_ns = int(datetime_to_ns(prepared_at))
    database.update(task)

    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.PREPARED
    assert job.updated_at == int(prepared_at.timestamp())
    assert job.status_code_updated_at == datetime_to_ns(prepared_at)

    # task hasn't changed, move forward > 60s so we will update
    mock_now1 = datetime.datetime(2025, 3, 1, 10, 7, 10, 99999)
    freezer.move_to(mock_now1)
    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.RUNNING
    assert job.status_code == StatusCode.PREPARED
    # Updated at has changes, status_code_updated_at remains at the prepared time
    assert job.updated_at == int(mock_now1.timestamp())
    assert job.status_code_updated_at == datetime_to_ns(prepared_at)


def test_job_definition_defaults(db):
    import time

    ts = int(time.time())
    job_request = job_request_factory(id="test_job_request")
    job = job_factory(id="test_job", job_request=job_request, created_at=ts)
    definition = main.job_to_job_definition(job, task_id="task_id")

    assert definition.to_dict() == {
        "action": "action_name",
        "allow_database_access": False,
        "args": [
            "myscript.py",
        ],
        "cpu_count": 2.0,
        "created_at": ts,
        "database_name": None,
        "env": {
            "OPENSAFELY_BACKEND": "test",
        },
        "id": "test_job",
        "image": "ghcr.io/opensafely-core/python",
        "input_job_ids": [],
        "inputs": [],
        "job_request_id": "test_job_request",
        "level4_file_types": [
            ".csv",
            ".html",
            ".jpeg",
            ".jpg",
            ".json",
            ".log",
            ".md",
            ".png",
            ".svg",
            ".svgz",
            ".txt",
        ],
        "level4_max_csv_rows": 5000,
        "level4_max_filesize": 16777216,
        "memory_limit": "4G",
        "output_spec": {},
        "study": {
            "branch": "main",
            "commit": "commit",
            "git_repo_url": "repo",
        },
        "task_id": "task_id",
        "workspace": "workspace",
    }


@pytest.mark.parametrize(
    "run_command,expect_env",
    [
        ("stata-mp:latest analysis/analyse.do", True),
        ("erhql:v1 analysis/dataset_definition.py", False),
    ],
)
def test_job_definition_stata_license(db, monkeypatch, run_command, expect_env):
    monkeypatch.setattr(config, "STATA_LICENSE", "dummy-license")
    job = job_factory(run_command=run_command)
    job_definition = main.job_to_job_definition(job, task_id="")
    if expect_env:
        assert job_definition.env["STATA_LICENSE"] == "dummy-license"
    else:
        assert "STATA_LICENSE" not in job_definition.env


@pytest.mark.parametrize(
    "repo_url,expect_env",
    [
        ("https://github.com/opensafely/ok-repo", True),
        ("https://github.com/opensafely/not-ok-repo", False),
    ],
)
def test_job_definition_ehrql_event_level_access(db, monkeypatch, repo_url, expect_env):
    monkeypatch.setattr(
        config,
        "REPOS_WITH_EHRQL_EVENT_LEVEL_ACCESS",
        {"https://github.com/opensafely/ok-repo"},
    )
    job = job_factory(requires_db=True, repo_url=repo_url)
    job_definition = main.job_to_job_definition(job, task_id="")
    if expect_env:
        assert job_definition.env["EHRQL_ENABLE_EVENT_LEVEL_QUERIES"] == "True"
    else:
        assert "EHRQL_ENABLE_EVENT_LEVEL_QUERIES" not in job_definition.env


@pytest.mark.parametrize(
    "project,expected_env",
    [
        ("project-with-no-permissions", ""),
        ("project-with-some-permissions", "table1,table2"),
    ],
)
def test_job_definition_ehrql_permitted_tables(db, monkeypatch, project, expected_env):
    monkeypatch.setattr(
        controller.permissions.datasets,
        "PERMISSIONS",
        {
            "project-with-some-permissions": ["table1", "table2"],
        },
    )
    jr = job_request_factory(
        original={
            "created_by": "testuser",
            "project": project,
            "orgs": ["org1", "org2"],
            "backend": "test",
            "workspace": {
                "name": "workspace",
                "repo": "repo",
                "commit": "commit",
                "branch": "main",
            },
        }
    )

    job = job_factory(requires_db=True, job_request=jr)
    job_definition = main.job_to_job_definition(job, task_id="")
    assert job_definition.env["EHRQL_PERMITTED_TABLES"] == expected_env


@patch("controller.main.handle_job")
def test_handle_error(patched_handle_job, db, monkeypatch):
    monkeypatch.setattr(common_config, "JOB_LOOP_INTERVAL", 0)

    # mock 2 controller loops, successful first pass and an
    # exception on the second loop
    patched_handle_job.side_effect = [None, Exception("test_hard_failure")]
    job = job_factory()

    with pytest.raises(Exception, match="test_hard_failure"):
        main.main()

    job = database.find_one(Job, id=job.id)
    assert job.state == State.FAILED
    assert job.status_code == StatusCode.INTERNAL_ERROR

    spans = get_trace("loop")
    span = spans[-2]  # final span is LOOP, we want last LOOP_JOB
    assert span.name == "LOOP_JOB"
    assert span.status.status_code.name == "ERROR"
    assert span.status.description == "Exception: test_hard_failure"
    assert span.attributes["job.fatal_error"] is True


@pytest.mark.parametrize(
    "exc",
    [
        sqlite3.OperationalError("database locked"),
        AssertionError("a bad thing"),
    ],
)
@patch("controller.main.handle_job")
def test_handle_non_fatal_error(patched_handle_job, db, monkeypatch, exc):
    monkeypatch.setattr(common_config, "JOB_LOOP_INTERVAL", 0)

    # mock 2 controller loops, successful first pass and an
    # exception on the second loop
    patched_handle_job.side_effect = [None, exc]
    job = job_factory()

    with pytest.raises(Exception, match=str(exc)):
        main.main()

    job = database.find_one(Job, id=job.id)
    # Job should still be pending
    assert job.state == State.PENDING

    spans = get_trace("loop")
    span = spans[-2]  # final span is LOOP, we want last LOOP_JOB
    assert span.name == "LOOP_JOB"
    assert len(span.events) == 1
    assert str(exc) in span.events[0].attributes["exception.message"]
    assert span.status.status_code.name == "ERROR"
    assert span.status.description == f"{exc.__class__.__name__}: {str(exc)}"
    assert span.attributes["job.fatal_error"] is False


def test_update_scheduled_task_for_db_maintenance(db, monkeypatch, freezer):
    monkeypatch.setattr(config, "MAINTENANCE_ENABLED_BACKENDS", ["test"])
    # We start with no DBSTATUS tasks
    tasks = database.find_where(Task, type=TaskType.DBSTATUS)
    assert len(tasks) == 0

    # Running the controller loop should automatically schedule a DBSTATUS task
    run_controller_loop_once()
    tasks = database.find_where(Task, type=TaskType.DBSTATUS)
    assert len(tasks) == 1

    # It should have the attributes we expect
    assert tasks[0].backend == "test"
    assert tasks[0].definition == {"database_name": "default"}

    # Running it again should not create another
    run_controller_loop_once()
    tasks = database.find_where(Task, type=TaskType.DBSTATUS)
    assert len(tasks) == 1

    # Mark the task as complete
    task_api.handle_task_update(
        task_id=tasks[0].id, stage="", results={}, complete=True
    )

    # Tick time forward a small amount and run the loop again which should _still_ not
    # create a new task because the previous one ran too recently
    freezer.tick(delta=1)
    run_controller_loop_once()
    tasks = database.find_where(Task, type=TaskType.DBSTATUS)
    assert len(tasks) == 1

    # After ticking time forward a significant amount, running the loop should now
    # create a new active task
    freezer.tick(delta=10000)
    run_controller_loop_once()
    tasks = database.find_where(Task, type=TaskType.DBSTATUS)
    assert len(tasks) == 2
    assert tasks[0].active is False
    assert tasks[1].active is True

    # Enable manual database maintenance mode
    set_flag("manual-db-maintenance", "true", backend="test")

    # Now running the loop should deactivate any active tasks
    run_controller_loop_once()
    tasks = database.find_where(Task, type=TaskType.DBSTATUS)
    assert len(tasks) == 2
    assert tasks[0].active is False
    assert tasks[1].active is False


# This is more of an integration test of both the controller and agent working together,
# rather than just a test of `handle_task_update_dbstatus()`. But I feel more confident
# in the code by exercising both elements, and there didn't feel like an obvious
# alternative place to put this test.
@patch("agent.main.docker", autospec=True)
def test_handle_task_update_dbstatus(
    mock_docker, monkeypatch, db, freezer, responses, live_server
):
    responses.add_passthru(live_server.url)
    backend = "test"
    monkeypatch.setattr(config, "MAINTENANCE_ENABLED_BACKENDS", [backend])
    monkeypatch.setattr(agent_config, "BACKEND", backend)
    monkeypatch.setattr(agent_config, "DATABASE_URLS", {"default": "mssql://localhost"})
    # Use the live_server url for our task api endpoint, for the agent to call in`run_agent_loop_once`
    monkeypatch.setattr("agent.config.TASK_API_ENDPOINT", live_server.url)
    # Ensure we have correct auth for the task api
    monkeypatch.setattr("agent.config.TASK_API_TOKEN", "test_token")
    monkeypatch.setattr("controller.config.JOB_SERVER_TOKENS", {"test": "test_token"})

    # We start not in maintenance mode
    assert not get_flag_value("mode", backend=backend)

    # Run controller loop to schedule a DBSTATUS task
    run_controller_loop_once()

    # Run agent loop to execute it with a mocked docker response
    mock_docker.return_value = Mock(stdout="db-maintenance")
    run_agent_loop_once()

    # We should now be in maintenance mode
    assert get_flag_value("mode", backend=backend) == "db-maintenance"

    # Jump forward in time
    freezer.tick(delta=1000)

    # Run controller loop to schedule another DBSTATUS task
    run_controller_loop_once()

    # Run agent loop to execute it with a mocked docker response
    mock_docker.return_value = Mock(stdout="")
    run_agent_loop_once()

    # We should now be out of maintenance mode
    assert not get_flag_value("mode", backend=backend)
