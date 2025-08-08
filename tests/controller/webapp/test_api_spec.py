from collections import defaultdict
from pathlib import Path

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
        f"{live_server.url}/api_spec.json", config=config
    )


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setattr("common.config.BACKENDS", ["test"])
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"token": ["test"]})


class Recorder:
    _inputs = set()
    status_codes = defaultdict(set)

    def record_status_code(self, path, status_code):
        self.status_codes[path].add(status_code)


@pytest.fixture(scope="module")
def recorder(request):
    default_expected_status_codes = {200, 401, 405}
    expected_status_codes_by_path = {
        "/{backend}/backend/status/": {200, 401, 404, 405},
        "/rap/cancel/": {200, 400, 401, 403, 404, 405},
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


schema = schemathesis.pytest.from_fixture("api_schema")


@pytest.fixture(autouse=True)
def setup_job(db):
    # Set up a job that matches the example request body for the /cancel
    # endpoint in the api spec; this allows us to hit the 200 response
    if not database.exists_where(Job, job_request_id="a1b2c3d4e5f6g7h8"):
        job_req = job_request_factory(id="a1b2c3d4e5f6g7h8")
        job_factory(state=State.PENDING, action="foo", job_request=job_req)


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
    response = client.get("/api-docs/")
    assert response.status_code == 200
