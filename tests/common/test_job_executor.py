import json

from common.job_executor import JobDefinition
from controller.main import job_to_job_definition
from tests.factories import job_factory


def test_job_executor_json_roundtrip(db):
    definition = job_to_job_definition(job_factory(), task_id="")
    serialized = json.dumps(definition.to_dict())
    deserialized = JobDefinition.from_dict(json.loads(serialized))
    assert definition == deserialized


# TODO: remove once new definition migrated
def test_job_executor_handles_old_json(db):
    definition = job_to_job_definition(job_factory(), task_id="")
    # old task definitions won't have image_sha
    task_definition = definition.to_dict()
    task_definition.pop("image_sha")

    serialized = json.dumps(task_definition)
    deserialized = JobDefinition.from_dict(json.loads(serialized))
    assert definition == deserialized
