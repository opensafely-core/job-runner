import datetime
import logging
import re

import pytest
from opentelemetry import trace

from jobrunner import config
from jobrunner.agent import task_api as agent_task_api
from jobrunner.controller import main
from jobrunner.controller.main import PlatformError
from jobrunner.lib import database
from jobrunner.models import Job, State, StatusCode, Task, TaskType
from jobrunner.queries import set_flag
from tests.conftest import get_trace
from tests.factories import job_factory, job_results_factory


def run_controller_loop_once():
    main.main(exit_callback=lambda _: True)


def set_job_task_results(job, job_results, error=None):
    runjob_task = database.find_one(
        Task, type=TaskType.RUNJOB, id__like=f"{job.id}-%", active=True
    )
    agent_task_api.update_controller(
        runjob_task,
        stage="",
        results={"results": job_results.to_dict(), "error": error},
        complete=True,
    )


def test_handle_pending_job_cancelled(db):
    job = job_factory()
    run_controller_loop_once()

    tasks = database.find_all(Task)
    assert len(tasks) == 1
    assert tasks[0].type == TaskType.RUNJOB
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
    assert spans[-3].name == "CREATED"
    assert spans[-2].name == "DEPENDENCY_FAILED"
    assert spans[-2].status.status_code == trace.StatusCode.ERROR
    assert spans[-1].name == "JOB"
    assert spans[-1].status.status_code == trace.StatusCode.ERROR


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
    assert spans[-1].name == "CREATED"


def test_handle_job_waiting_on_workers(monkeypatch, db):
    monkeypatch.setattr(config, "MAX_WORKERS", 0)

    job = job_factory()
    run_controller_loop_once()

    tasks = database.find_all(Task)
    job = database.find_one(Job, id=job.id)

    assert len(tasks) == 0
    assert job.state == State.PENDING
    assert job.status_message == "Waiting on available workers"
    assert job.status_code == StatusCode.WAITING_ON_WORKERS

    # tracing
    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_job_waiting_on_workers_resource_intensive_job(monkeypatch, db):
    monkeypatch.setattr(config, "MAX_WORKERS", 2)
    monkeypatch.setattr(
        config, "JOB_RESOURCE_WEIGHTS", {"workspace": {re.compile(r"action\d{1}"): 1.5}}
    )

    # This action requires 1.5 workers, set requires_db to ensure it's the first one run
    job1 = job_factory(workspace="workspace", action="action1", requires_db=True)
    # This action requires 1.5 workers, only 0.5 left after first one is running
    job2 = job_factory(workspace="workspace", action="action2")
    # This action requires 1 worker, only 0.5 left after first one is running
    job3 = job_factory(workspace="workspace", action="non_matching_action")
    run_controller_loop_once()

    job1 = database.find_one(Job, id=job1.id)
    job2 = database.find_one(Job, id=job2.id)
    job3 = database.find_one(Job, id=job3.id)

    assert job1.state == State.RUNNING
    assert job1.status_code == StatusCode.EXECUTING

    assert job2.state == State.PENDING
    assert (
        job2.status_message == "Waiting on available workers for resource intensive job"
    )
    assert job2.status_code == StatusCode.WAITING_ON_WORKERS

    assert job3.state == State.PENDING
    assert job3.status_message == "Waiting on available workers"
    assert job3.status_code == StatusCode.WAITING_ON_WORKERS


def test_handle_job_waiting_on_db_workers(monkeypatch, db):
    monkeypatch.setattr(config, "MAX_DB_WORKERS", 0)
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
    )
    run_controller_loop_once()

    tasks = database.find_all(Task)
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
        job_results_factory(
            outputs={"output/output.csv": "moderately_sensitive"},
            level4_excluded_files={"output/output.csv": "too big"},
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
        job_results_factory(
            outputs={"output/file.csv": "highly_sensitive"},
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
    assert job.outputs == {"output/file.csv": "highly_sensitive"}

    spans = get_trace("jobs")
    completed_span = spans[-2]
    assert completed_span.name == "NONZERO_EXIT"
    assert completed_span.attributes["exit_code"] == exit_code
    assert completed_span.attributes["outputs"] == 1
    assert completed_span.attributes["unmatched_patterns"] == 0
    assert completed_span.attributes["unmatched_outputs"] == 0
    assert completed_span.attributes["image_id"] == "image_id"
    assert completed_span.status.status_code == trace.StatusCode.ERROR
    assert spans[-1].name == "JOB"


def test_handle_job_finalized_failed_unmatched_patterns(db):
    job = job_factory()
    run_controller_loop_once()
    set_job_task_results(
        job,
        job_results_factory(
            outputs={"output/file.csv": "highly_sensitive"},
            unmatched_patterns=["badfile.csv"],
            unmatched_outputs=["otherbadfile.csv"],
        ),
    )
    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    # our state
    assert job.state == State.FAILED
    assert job.status_message == "No outputs found matching patterns:\n - badfile.csv"
    assert job.outputs == {"output/file.csv": "highly_sensitive"}
    assert job.unmatched_outputs == ["otherbadfile.csv"]

    spans = get_trace("jobs")
    completed_span = spans[-2]
    assert completed_span.name == "UNMATCHED_PATTERNS"
    assert completed_span.attributes["outputs"] == 1
    assert completed_span.attributes["unmatched_patterns"] == 1
    assert completed_span.attributes["unmatched_outputs"] == 1
    assert spans[-1].name == "JOB"


def test_handle_job_finalized_failed_with_error(db):
    # insert previous outputs
    # create new job
    job = job_factory()

    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)
    assert job.state == State.RUNNING

    set_job_task_results(job, job_results_factory(), error=str(Exception("foo")))

    with pytest.raises(PlatformError):
        run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    # our state
    assert job.state == State.FAILED
    assert job.status_code == StatusCode.INTERNAL_ERROR
    assert "Internal error" in job.status_message


@pytest.fixture
def backend_db_config(monkeypatch):
    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    # for test jobs, job.database_name is None, so add a dummy connection
    # string for that db
    monkeypatch.setitem(config.DATABASE_URLS, None, "conn str")


def test_handle_pending_db_maintenance_mode(db, backend_db_config):
    set_flag("mode", "db-maintenance")
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
    )

    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)

    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_DB_MAINTENANCE
    assert job.status_message == "Waiting for database to finish maintenance"
    assert job.started_at is None

    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_pending_cancelled_db_maintenance_mode(db, backend_db_config):
    set_flag("mode", "db-maintenance")
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
        cancelled=True,
    )

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

    set_flag("mode", "db-maintenance")
    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)

    # job has been set back to pending
    assert job.state == State.PENDING
    assert job.status_code == StatusCode.WAITING_DB_MAINTENANCE
    assert job.status_message == "Waiting for database to finish maintenance"
    assert job.started_at is None

    # the RUNJOB task is no longer active and a new CANCELJOB task has been created
    tasks = database.find_all(Task)
    assert len(tasks) == 2
    assert tasks[0].type == TaskType.RUNJOB
    assert not tasks[0].active
    assert tasks[1].type == TaskType.CANCELJOB
    assert tasks[1].active


def test_handle_pending_pause_mode(db, backend_db_config):
    set_flag("paused", "True")
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
    )

    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)

    assert job.state == State.PENDING
    assert job.started_at is None
    assert "paused" in job.status_message

    spans = get_trace("jobs")
    assert spans[-1].name == "CREATED"


def test_handle_running_pause_mode(db, backend_db_config):
    job = job_factory(
        run_command="ehrql:v1 generate-dataset dataset.py --output data.csv",
        requires_db=True,
    )

    # Start it running, then pause, then update its status
    run_controller_loop_once()
    set_flag("paused", "True")
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
        requires_outputs_from=["other-action"],
    )
    run_controller_loop_once()

    task = database.find_one(Task)
    assert task.definition["inputs"] == ["output-from-completed-run"]


def test_job_definition_limits(db):
    job = job_factory()
    job_definition = main.job_to_job_definition(job)
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


def test_mark_job_as_failed_adds_error(db):
    job = job_factory()
    main.mark_job_as_failed(job, StatusCode.INTERNAL_ERROR, "error")

    # tracing
    spans = get_trace("jobs")
    assert spans[-3].name == "CREATED"
    assert spans[-2].name == "INTERNAL_ERROR"
    assert spans[-2].status.status_code == trace.StatusCode.ERROR
    assert spans[-1].name == "JOB"
    assert spans[-1].status.status_code == trace.StatusCode.ERROR
