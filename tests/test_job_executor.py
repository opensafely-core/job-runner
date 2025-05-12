import json
import time

import pytest

from jobrunner.controller.main import job_to_job_definition
from jobrunner.job_executor import JobDefinition, JobResults
from tests.factories import job_factory, job_results_factory


def test_job_executor_json_roundtrip(db):
    definition = job_to_job_definition(job_factory())
    serialized = json.dumps(definition.to_dict())
    deserialized = JobDefinition.from_dict(json.loads(serialized))
    assert definition == deserialized


@pytest.mark.parametrize("exit_code", [None, "None"])
def test_empty_job_results_json_roundtrip(db, exit_code):
    job_results = job_results_factory(
        outputs={},
        unmatched_patterns=[],
        unmatched_outputs=[],
        unmatched_hint=None,
        exit_code=exit_code,
        image_id=None,
        timestamp_ns=time.time_ns(),
        action_version=None,
        action_revision=None,
        action_created=None,
        base_revision=None,
        base_created=None,
        message="Job error",
    )

    serialized = json.dumps(job_results.to_dict())
    deserialized = JobResults.from_dict(json.loads(serialized))

    # if exit code is a string, it is deserialized to None
    job_results.exit_code = None
    assert job_results == deserialized
