from collections import defaultdict
from pathlib import Path

import hypothesis
import pytest
import responses
import schemathesis

from controller.lib import database
from controller.models import Job, State
from controller.webapp.api_spec.utils import load_api_spec_json
from tests.factories import job_factory, job_request_factory


FIXTURES_PATH = Path(__file__).parent.parent.parent.resolve() / "fixtures"


@schemathesis.hook
def before_add_examples(context, examples: list[schemathesis.Case]):
    # Modify the rap/create/ example2 request body to use our fixture repo, so the
    # jobs will be successfully created
    for example in examples:
        if example.body.get("repo_url") == "https://github.com/opensafely/a-new-study":
            example.body["repo_url"] = str(FIXTURES_PATH / "git-repo")


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
    # allowed access another known backend (test1)
    # In our test setup, we create test jobs matching the values from the api spec,
    # with either the test backend (to test happy paths) or test1 backend (to test not allowed)
    monkeypatch.setattr("common.config.BACKENDS", ["test", "test1"])
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"token": ["test"]})


class Recorder:
    expected_status_codes_by_path = {
        "/backend/status/": {200, 401},
        "/rap/cancel/": {200, 400, 401, 404},
        "/rap/create/": {200, 201, 400, 401},
        "/rap/status/": {200, 400, 401},
    }
    status_codes = defaultdict(set)
    count = 0

    def record_status_code(self, path, status_code):
        self.count += 1
        self.status_codes[path].add(status_code)


@pytest.fixture(scope="module")
def recorder(request):
    recorder_ = Recorder()

    yield recorder_

    # The schemathesis pytest integration has fewer features than the cli. If it doesn't generate
    # tests that hit all status codes (e.g. if it never has valid auth), the tests will still pass
    # (the CLI tests will catch this)
    # So, we record all status codes seen for each path and ensure we've seen all the expected ones
    # and haven't seen any we didn't expect
    for path, status_codes in recorder_.status_codes.items():
        expected_status_codes = recorder_.expected_status_codes_by_path[path]
        missed_codes = expected_status_codes - status_codes
        assert not missed_codes, (
            f"Expected status codes not tested for path {path}: {missed_codes}"
        )

        # 405 is not explicitly specified in the spec; we may encounter it if a
        # view requires certain methods, but it's not especially interesting, so we
        # ignore it
        extra_codes = status_codes - expected_status_codes - {405}
        assert not extra_codes, (
            f"Unexpected status codes found for path {path}: {extra_codes}"
        )
    print(f"Cases: {recorder_.count}")


schema = schemathesis.pytest.from_fixture("api_schema")


@pytest.fixture(autouse=True)
def setup_jobs(db):
    # Set up jobs that match the request body for the
    # endpoint examples in the api spec;
    # this allows us to hit the 200/201 responses for creating and cancelling
    example_jobs = [
        # successful rap/cancel/ example1
        ("a1b2c3d4e5f6g7h8", ["action1"], "test"),
        # error rap/cancel/ example2, non allowed backend
        ("abcdefgh12345678", ["action2", "action3"], "test1"),
        # rap/create/ example1, jobs already created
        ("abcdefgh23456789", ["action1"], "test"),
    ]
    for rap_id, actions, backend in example_jobs:
        if not database.exists_where(Job, rap_id=rap_id):
            job_req = job_request_factory(id=rap_id)
            for action in actions:
                job_factory(
                    state=State.PENDING,
                    action=action,
                    job_request=job_req,
                    backend=backend,
                )


def test_expected_status_codes():
    api_spec = load_api_spec_json()
    status_codes_from_spec = {}
    for path, spec in api_spec["paths"].items():
        status_codes = set()
        for method in ["get", "post", "put", "patch", "delete"]:
            if method in spec:
                for status_code in spec[method]["responses"]:
                    status_codes.add(status_code)
        status_codes_from_spec[path] = status_codes

    assert status_codes_from_spec == Recorder.expected_status_codes_by_path


@hypothesis.settings(deadline=None)
@schema.parametrize()
def test_api_with_auth(db, case, recorder):
    # We pass good headers; schemathesis will typically generate a test case
    # with bad auth too, so the 401 status is covered
    case.headers = {"Authorization": "token"}
    call_and_validate(case, recorder)


# Tests for bad/no auth
# We only test in the explicit phase (i.e. examples) for no/bad tokens, since
# we expect them always to error
@hypothesis.settings(deadline=None, phases=[hypothesis.Phase.explicit])
@schema.parametrize()
def test_api_no_token(db, case, recorder):
    # We pass good headers; schemathesis will typically generate a test case
    # with bad auth too, so the 401 status is covered
    case.headers = {}
    call_and_validate(case, recorder)


@hypothesis.settings(deadline=None, phases=[hypothesis.Phase.explicit])
@schema.parametrize()
def test_api_bad_token(db, case, recorder):
    case.headers = {"Authorization": "bad-token"}
    call_and_validate(case, recorder)


def call_and_validate(case, recorder):
    # Note: we're not using case.call_and_validate() here so that we can record the status
    # code prior to validating the response (otherwise status codes for check failures
    # won't be recorded).
    response = case.call()
    recorder.record_status_code(case.path, response.status_code)
    case.validate_response(response)


def test_api_docs(client):
    response = client.get("/controller/v1/docs/")
    assert response.status_code == 200
