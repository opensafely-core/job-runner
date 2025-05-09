from datetime import datetime

from jobrunner.schema import TaskType
from tests.factories import (
    canceljob_db_task_factory,
    job_factory,
    runjob_db_task_factory,
)


def test_job_asdict_timestamps(db):
    actual_time = "2022-10-07T14:59:12.345678+0000"
    fmt = "%Y-%m-%dT%H:%M:%S.%f%z"
    dt = datetime.strptime(actual_time, fmt)
    ts = dt.timestamp()

    job = job_factory(created_at=int(ts), status_code_updated_at=int(ts * 1e9))
    d = job.asdict()

    assert d["created_at"] == "2022-10-07T14:59:12Z"
    assert d["status_code_updated_at"] == "2022-10-07T14:59:12.345678Z"


def test_task_asdict(db):
    run_task = runjob_db_task_factory()
    cancel_task = canceljob_db_task_factory()

    run_task_dict = run_task.asdict()
    cancel_task_dict = cancel_task.asdict()

    assert run_task_dict["type"] == TaskType.RUNJOB.value
    assert cancel_task_dict["type"] == TaskType.CANCELJOB.value
