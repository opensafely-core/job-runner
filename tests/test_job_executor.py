import json

from jobrunner.controller.main import job_to_job_definition
from jobrunner.job_executor import JobDefinition
from tests.factories import job_factory


def test_job_executor_json_roundtrip(db):
    definition = job_to_job_definition(job_factory(), task_id="")
    serialized = json.dumps(definition.to_dict())
    deserialized = JobDefinition.from_dict(json.loads(serialized))
    assert definition == deserialized
