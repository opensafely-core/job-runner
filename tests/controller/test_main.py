import pytest
from opentelemetry import trace

from jobrunner import config, run
from jobrunner.agent import task_api as agent_task_api
from jobrunner.controller import main
from jobrunner.job_executor import ExecutorState, JobStatus
from jobrunner.lib import database
from jobrunner.models import Job, State, StatusCode, Task, TaskType
from jobrunner.queries import set_flag
from tests.conftest import get_trace
from tests.factories import job_factory, job_results_factory
from tests.fakes import RecordingExecutor


def run_controller_loop_once():
    main.main(exit_callback=lambda _: True)


def set_job_task_results(job, job_results):
    runjob_task = database.find_one(
        Task, type=TaskType.RUNJOB, id__like=f"{job.id}-%", active=True
    )
    agent_task_api.update_controller(
        runjob_task,
        stage="",
        results={"results": job_results.to_dict(), "error": None},
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
    "exit_code,run_command,extra_message",
    [
        (
            3,
            "cohortextractor generate_cohort",
            (
                "A transient database error occurred, your job may run "
                "if you try it again, if it keeps failing then contact tech support"
            ),
        ),
        (
            4,
            "cohortextractor generate_cohort",
            "New data is being imported into the database, please try again in a few hours",
        ),
        (
            5,
            "cohortextractor generate_cohort",
            "Something went wrong with the database, please contact tech support",
        ),
        # the same exit codes for a job that doesn't have access to the database show no message
        (3, "python foo.py", None),
        (4, "python foo.py", None),
        (5, "python foo.py", None),
    ],
)
def test_handle_job_finalized_failed_exit_code(
    exit_code, run_command, extra_message, db, backend_db_config
):
    job = job_factory(
        run_command=run_command,
        requires_db="cohortextractor" in run_command,
    )

    run_controller_loop_once()
    set_job_task_results(
        job,
        job_results_factory(
            outputs={"output/file.csv": "highly_sensitive"},
            exit_code=exit_code,
            message=None,
        ),
    )
    run_controller_loop_once()

    job = database.find_one(Job, id=job.id)

    # our state
    assert job.state == State.FAILED
    assert job.status_code == StatusCode.NONZERO_EXIT
    expected = "Job exited with an error"
    if extra_message:
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


@pytest.fixture
def backend_db_config(monkeypatch):
    monkeypatch.setattr(config, "USING_DUMMY_DATA_BACKEND", False)
    # for test jobs, job.database_name is None, so add a dummy connection
    # string for that db
    monkeypatch.setitem(config.DATABASE_URLS, None, "conn str")


def test_handle_pending_db_maintenance_mode(db, backend_db_config):
    set_flag("mode", "db-maintenance")
    job = job_factory(
        run_command="cohortextractor:latest generate_cohort",
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
        run_command="cohortextractor:latest generate_cohort",
        requires_db=True,
        cancelled=True,
    )

    run_controller_loop_once()
    job = database.find_one(Job, id=job.id)

    assert job.state == State.FAILED
    assert job.status_code == StatusCode.CANCELLED_BY_USER
    assert job.status_message == "Cancelled by user"
    assert job.started_at is None


def test_handle_pending_pause_mode(db, backend_db_config):
    set_flag("paused", "True")
    job = job_factory(
        run_command="cohortextractor:latest generate_cohort",
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
        run_command="cohortextractor:latest generate_cohort",
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

    api = RecordingExecutor(
        JobStatus(ExecutorState.UNKNOWN), JobStatus(ExecutorState.PREPARING)
    )
    run.handle_job(
        job_factory(
            id="3", requires_outputs_from=["other-action"], state=State.PENDING
        ),
        api,
    )

    assert api.job_definition.inputs == ["output-from-completed-run"]


def test_job_definition_limits(db):
    job = job_factory()
    job_definition = main.job_to_job_definition(job)
    assert job_definition.cpu_count == 2
    assert job_definition.memory_limit == "4G"


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
