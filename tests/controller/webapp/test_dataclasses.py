import re
from pathlib import Path

import pytest

from controller.create_or_update_jobs import create_jobs
from controller.lib.database import find_all
from controller.models import Job
from controller.webapp.views.validators.dataclasses import CreateRequest, RequestBody
from controller.webapp.views.validators.exceptions import APIValidationError
from tests.factories import job_request_rap_api_v1_factory_raw


FIXTURES_PATH = Path(__file__).parent.parent.parent.resolve() / "fixtures"


@pytest.fixture
def mock_api_schema(monkeypatch):
    test_schema = {
        "type": "object",
        "required": ["foo", "bar", "baz"],
        "properties": {
            "foo": {
                "type": "object",
                "required": ["foo_array"],
                "properties": {
                    "foo_array": {
                        "type": "array",
                        "minItems": 2,
                        "items": {
                            "type": "number",
                        },
                    },
                    "not_required": {"type": "string"},
                },
            },
            "bar": {"type": "string", "enum": ["a", "b", "c"]},
            "baz": {
                "type": "array",
                "items": {"type": "string", "pattern": "^[a-z]{4}$"},
            },
        },
    }

    api_spec_json = {"components": {"schemas": {"testSchema": test_schema}}}
    monkeypatch.setattr(
        "controller.webapp.views.validators.dataclasses.api_spec_json", api_spec_json
    )
    yield


@pytest.mark.parametrize(
    "body,error",
    [
        # Validation fails on first required property
        ({}, "'foo' is a required property"),
        # Validation checks top level required properties first
        ({"foo": []}, "'bar' is a required property"),
        # Top level required properties present, fails on foo type. Error message contains json path to problem property
        (
            {"foo": [], "bar": "a", "baz": []},
            "Invalid request body received at $.foo: [] is not of type 'object'",
        ),
        # Nested required property missing; error message contains json path to problem property
        (
            {"foo": {"not_required": ""}, "bar": "a", "baz": []},
            "Invalid request body received at $.foo: 'foo_array' is a required property",
        ),
        # Array length error
        (
            {"foo": {"foo_array": [1]}, "bar": "a", "baz": []},
            "Invalid request body received at $.foo.foo_array: [1] is too short",
        ),
        # Type error
        (
            {"foo": {"foo_array": ["a", 2]}, "bar": "a", "baz": []},
            "Invalid request body received at $.foo.foo_array[0]: 'a' is not of type 'number'",
        ),
        # Pattern match error
        (
            {"foo": {"foo_array": [1, 2]}, "bar": "a", "baz": ["ab12", "AAA"]},
            "Invalid request body received at $.baz[1]: 'AAA' does not match",
        ),
    ],
)
def test_validate_schema_error_message(mock_api_schema, body, error):
    with pytest.raises(APIValidationError, match=re.escape(error)):
        RequestBody.validate_schema(body, "testSchema")


def test_can_create_jobs(tmp_work_dir, db):
    # Test that the CreateRequestBody object that we receive in the new
    # create view can act as a JobRequest and be passed to create_jobs
    repo_url = str(FIXTURES_PATH / "git-repo")

    job_request_body = job_request_rap_api_v1_factory_raw(
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit="d090466f63b0d68084144d8f105f0d6e79a0819e",
        branch="v1",
        requested_actions=["generate_dataset"],
    )
    job_request = CreateRequest.from_request(job_request_body)
    assert job_request.original == {
        **job_request_body,
        "workspace": {
            "name": job_request_body["workspace"],
            "branch": job_request_body["branch"],
        },
    }
    create_jobs(job_request)
    jobs = find_all(Job)
    assert len(jobs) == 1
    assert jobs[0].job_request_id == job_request_body["job_request_id"]
