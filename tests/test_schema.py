import json
import time

import pytest

from jobrunner.schema import TaskResults
from tests.factories import task_results_factory


@pytest.mark.parametrize("exit_code", [None, "None"])
def test_empty_job_results_json_roundtrip(db, exit_code):
    task_results = task_results_factory(
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
        has_unmatched_patterns=False,
        has_level4_excluded_files=False,
    )

    serialized = json.dumps(task_results.to_dict())
    deserialized = TaskResults.from_dict(json.loads(serialized))

    # if exit code is a string, it is deserialized to None
    task_results.exit_code = None
    assert task_results == deserialized
