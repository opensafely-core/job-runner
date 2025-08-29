from collections import defaultdict
from pathlib import Path

import hypothesis
import pytest
import responses
import schemathesis

from controller.lib import database
from controller.models import Job, State
from tests.factories import job_factory, job_request_factory


@pytest.fixture
def api_schema(live_server):
    responses.add_passthru(live_server.url)
    config = schemathesis.Config.from_path(
        Path(__file__).parents[3]
        / "controller"
        / "webapp"
        / "api_spec"
        / "schemathesis.toml"
    )
    return schemathesis.openapi.from_url(
        f"{live_server.url}/controller/v1/spec.json", config=config
    )


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    # Schemathesis will use the example values we include in the api spec
    # set up config so this token has access to one known backend (test), and is not
    # allowed access another known backend (foo)
    # In our test setup, we create test jobs matching the values from the api spec,
    # with either the test backend (to test happy paths) or foo backend (to test not allowed)
    monkeypatch.setattr("common.config.BACKENDS", ["test", "foo"])
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"token": ["test"]})


class Recorder:
    _inputs = set()
    status_codes = defaultdict(set)
    count = 0

    def record_status_code(self, path, status_code):
        self.count += 1
        self.status_codes[path].add(status_code)


@pytest.fixture(scope="module")
def recorder(request):
    default_expected_status_codes = {200, 401, 405}
    expected_status_codes_by_path = {
        "/backend/status/": {200, 401, 405},
        "/rap/cancel/": {200, 400, 401, 405},
        "/rap/create/": {200, 400, 401, 403, 405},
        "/rap/status/": {200, 400, 401, 405},
    }

    recorder_ = Recorder()

    yield recorder_

    # The schemathesis pytest integration has fewer features than the cli. If it doesn't generate
    # tests that hit all status codes (e.g. if it never has valid auth), the tests will still pass
    # (the CLI tests will catch this)
    # So, we record all status codes seen for each path and ensure we've seen all the expected ones
    # and haven't seen any we didn't expect
    for path, status_codes in recorder_.status_codes.items():
        expected_status_codes = expected_status_codes_by_path.get(
            path, default_expected_status_codes
        )
        missed_codes = expected_status_codes - status_codes
        assert not missed_codes, (
            f"Expected status codes not tested for path {path}: {missed_codes}"
        )

        extra_codes = status_codes - expected_status_codes
        assert not extra_codes, (
            f"Unexpected status codes found for path {path}: {extra_codes}"
        )
    print(f"Cases: {recorder_.count}")


schema = schemathesis.pytest.from_fixture("api_schema")


@pytest.fixture(autouse=True)
def setup_jobs(db):
    # Set up jobs that match the request body for the /cancel
    # endpoint examples in the api spec;
    # this allows us to hit the 200 response (jobs with test backend)
    # and 403 responses (jobs with not-allowed foo backend)
    example_jobs = [
        ("a1b2c3d4e5f6g7h8", ["action1"], "test"),
        ("abcdefgh12345678", ["action2", "action3"], "foo"),
    ]
    for job_request_id, actions, backend in example_jobs:
        if not database.exists_where(Job, job_request_id=job_request_id):
            job_req = job_request_factory(id=job_request_id)
            for action in actions:
                job_factory(
                    state=State.PENDING,
                    action=action,
                    job_request=job_req,
                    backend=backend,
                )


@hypothesis.settings(deadline=None)
@schema.parametrize()
def test_api_with_auth(db, case, recorder):
    # We pass good headers; schemathesis will typically generate a test case
    # with bad auth too, so the 401 status is covered
    case.headers = {"Authorization": "token"}
    call_and_validate(case, recorder)


def call_and_validate(case, recorder):
    # Note: we're not using call_and_validate here so that we can record the status
    # code prior to validating the response (otherwise status codes for check failures
    # won't be recorded).
    response = case.call()
    recorder.record_status_code(case.path, response.status_code)
    case.validate_response(response)


def test_api_docs(client):
    response = client.get("/controller/v1/docs/")
    assert response.status_code == 200
