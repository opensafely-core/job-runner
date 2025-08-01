from collections import defaultdict
from pathlib import Path

import hypothesis as hyp
import pytest
import responses
import schemathesis


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
    recorder_ = Recorder()

    yield recorder_

    # The schemathesis pytest integration has fewer features than the cli. If it doesn't generate
    # tests that hit all status codes (e.g. if it never has valid auth), the tests will still pass
    # (the CLI tests will catch this)
    # So, we record all status codes seen for each path and ensure we've seen all the expected ones
    # and haven't seen any we didn't expect
    # Note that currently the schemathesis.toml ignores 500 checks due to opentelemetry-django issue
    # with certain unicode characters
    expected_status_codes = {200, 401, 404}
    for path, status_codes in recorder_.status_codes.items():
        missed_codes = expected_status_codes - status_codes
        assert not missed_codes, (
            f"Expected status codes not tested for path {path}: {missed_codes}"
        )

        extra_codes = status_codes - expected_status_codes
        assert not extra_codes, (
            f"Unexpected status codes found for path {path}: {extra_codes}"
        )


schema = schemathesis.pytest.from_fixture("api_schema")


@schema.parametrize()
# Generate authentication tokens
@schema.given(
    auth_token=hyp.strategies.sampled_from(["token", "bad-token", None]),
)
def test_api_with_auth(db, case, recorder, auth_token):
    if auth_token is not None:
        case.headers = {"Authorization": auth_token}
    response = case.call_and_validate()
    print(response.status_code)
    recorder.record_status_code(case.path, response.status_code)
