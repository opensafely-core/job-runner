import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from django.urls import reverse
from pipeline import load_pipeline

from common.lib.git import read_file_from_repo
from controller.lib.database import find_one, find_where
from controller.models import Job, State, StatusCode, timestamp_to_isoformat
from controller.queries import set_flag
from controller.webapp.views.rap_views import job_to_api_format
from tests.conftest import get_trace
from tests.factories import (
    job_factory,
    job_request_factory,
    rap_api_v1_factory_raw,
    runjob_db_task_factory,
)


FIXTURES_PATH = Path(__file__).parent.parent.parent.resolve() / "fixtures"


# use a fixed time for these tests
TEST_TIME = time.time()
TEST_DATESTR = timestamp_to_isoformat(TEST_TIME)


def setup_auto_tracing():
    from opentelemetry.instrumentation.auto_instrumentation import (  # noqa: F401
        sitecustomize,
    )


@pytest.mark.parametrize(
    "flags_to_set, expected_backend_response",
    [
        # case 1: last seen set for backend
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
            ],
            [
                {
                    "slug": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": None},
                    "db_maintenance": {
                        "status": "off",
                        "since": None,
                        "type": None,
                    },
                },
            ],
        ),
        # case 2: backend paused
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", "true", "test_backend"),
            ],
            [
                {
                    "slug": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "on", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "off",
                        "since": None,
                        "type": None,
                    },
                },
            ],
        ),
        # case 3: backend not paused
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", None, "test_backend"),
            ],
            [
                {
                    "slug": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "off",
                        "since": None,
                        "type": None,
                    },
                },
            ],
        ),
        # case 4: backend in scheduled db maintenance
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", None, "test_backend"),
                ("mode", "db-maintenance", "test_backend"),
            ],
            [
                {
                    "slug": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "on",
                        "since": TEST_DATESTR,
                        "type": "scheduled",
                    },
                },
            ],
        ),
        # case 5: backend in manual db maintenance
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", None, "test_backend"),
                ("mode", "db-maintenance", "test_backend"),
                ("manual-db-maintenance", "on", "test_backend"),
            ],
            [
                {
                    "slug": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "on",
                        "since": TEST_DATESTR,
                        "type": "manual",
                    },
                },
            ],
        ),
        # case 6: backend has previously been in db maintenance
        (
            [
                ("last-seen-at", TEST_DATESTR, "test_backend"),
                ("paused", None, "test_backend"),
                ("mode", None, "test_backend"),
                ("manual-db-maintenance", None, "test_backend"),
            ],
            [
                {
                    "slug": "test_backend",
                    "last_seen": TEST_DATESTR,
                    "paused": {"status": "off", "since": TEST_DATESTR},
                    "db_maintenance": {
                        "status": "off",
                        "since": TEST_DATESTR,
                        "type": None,
                    },
                },
            ],
        ),
    ],
)
def test_backends_status_view(
    db, client, monkeypatch, freezer, flags_to_set, expected_backend_response
):
    freezer.move_to(TEST_DATESTR)
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS",
        {"test_token": ["test_backend"]},
    )
    headers = {"Authorization": "test_token"}

    # set flag for unauthorised backend
    set_flag("foo", "bar", "test_other_backend")
    # set flag for authorised backends
    for flag_id, value, backend in flags_to_set:
        set_flag(flag_id, value, backend)

    response = client.get(reverse("backends_status"), headers=headers)
    assert response.status_code == 200
    response_json = response.json()

    assert response_json["backends"] == expected_backend_response


def test_backends_status_view_no_flags(db, client, monkeypatch):
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS", {"test_token": ["test", "foo"]}
    )
    headers = {"Authorization": "test_token"}

    response = client.get(reverse("backends_status"), headers=headers)
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["backends"] == [
        {
            "slug": "test",
            "last_seen": None,
            "paused": {"status": "off", "since": None},
            "db_maintenance": {
                "status": "off",
                "since": None,
                "type": None,
            },
        },
        {
            "slug": "foo",
            "last_seen": None,
            "paused": {"status": "off", "since": None},
            "db_maintenance": {
                "status": "off",
                "since": None,
                "type": None,
            },
        },
    ]


def test_backends_status_view_tracing(db, client, monkeypatch):
    monkeypatch.setattr(
        "controller.config.CLIENT_TOKENS", {"test_token": ["test", "foo"]}
    )
    headers = {"Authorization": "test_token"}
    setup_auto_tracing()
    client.get(reverse("backends_status"), headers=headers)

    traces = get_trace()
    last_trace = traces[-1]
    # default django attributes
    assert last_trace.attributes["http.request.method"] == "GET"
    assert last_trace.attributes["http.route"] == ("controller/v1/backend/status/")
    assert last_trace.attributes["http.response.status_code"] == 200


def test_cancel_view(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1", backend="test")
    assert not job.cancelled
    post_data = {
        "rap_id": job.job_request_id,
        "actions": ["action1"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        "success": "ok",
        "details": "1 actions cancelled",
        "count": 1,
    }, response
    job = find_one(Job, id=job.id)
    assert job.cancelled


def test_cancel_view_multiple(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job_request = job_request_factory()
    job1 = job_factory(
        state=State.PENDING, action="action1", backend="test", job_request=job_request
    )
    job2 = job_factory(
        state=State.RUNNING, action="action2", backend="test", job_request=job_request
    )
    job3 = job_factory(
        state=State.RUNNING, action="action3", backend="test", job_request=job_request
    )
    jobs = (job1, job2, job3)
    assert all(not job.cancelled for job in jobs)
    post_data = {
        "rap_id": job_request.id,
        "actions": ["action1", "action2", "action3"],
    }

    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        "success": "ok",
        "details": "3 actions cancelled",
        "count": 3,
    }, response
    assert all(
        job.cancelled for job in find_where(Job, id__in=[job.id for job in jobs])
    )


def test_cancel_view_validation_error(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    post_data = {
        "actions": ["action1"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "Validation error",
        "details": "Invalid request body received: 'rap_id' is a required property",
    }


def test_cancel_view_no_jobs_for_rap_id(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    post_data = {
        "rap_id": "abcdefgh12345678",
        "actions": ["action1"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 404
    response_json = response.json()
    assert response_json == {
        "error": "jobs not found",
        "details": "No jobs found for rap_id abcdefgh12345678",
        "rap_id": "abcdefgh12345678",
    }


def test_cancel_view_actions_not_found(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1", backend="test")

    post_data = {
        "rap_id": job.job_request_id,
        "actions": ["action2", "action3"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 404
    response_json = response.json()
    assert response_json == {
        "error": "jobs not found",
        "details": "Jobs matching requested cancelled actions could not be found: action2,action3",
        "rap_id": job.job_request_id,
        "not_found": ["action2", "action3"],
    }


def test_cancel_view_not_allowed_for_backend(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1", backend="foo")

    post_data = {
        "rap_id": job.job_request_id,
        "actions": ["action1"],
    }
    response = client.post(
        reverse("cancel"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 404
    response_json = response.json()
    assert response_json == {
        "error": "jobs not found",
        "details": f"No jobs found for rap_id {job.job_request_id}",
        "rap_id": job.job_request_id,
    }


def test_create_view(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    repo_url = str(FIXTURES_PATH / "git-repo")

    rap_request_body = rap_api_v1_factory_raw(
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit="d090466f63b0d68084144d8f105f0d6e79a0819e",
        branch="v1",
        requested_actions=["generate_dataset"],
    )

    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 201
    response_json = response.json()
    assert response_json == {
        "result": "Success",
        "details": f"Jobs created for rap_id '{rap_request_body['rap_id']}'",
        "rap_id": rap_request_body["rap_id"],
        "count": 1,
    }, response
    job = find_one(Job, job_request_id=rap_request_body["rap_id"])
    assert job.action == "generate_dataset"


def test_create_view_validation_error(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    rap_request_body = rap_api_v1_factory_raw(requested_actions=[])
    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )

    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "Validation error",
        "details": "Invalid request body received at $.requested_actions: [] should be non-empty",
    }


def test_create_view_not_allowed_for_backend(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    rap_request_body = rap_api_v1_factory_raw(backend="foo")

    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400, response.json()
    response_json = response.json()
    assert response_json == {
        "error": "Error creating jobs",
        "details": "Unknown error",
    }


def test_create_view_jobs_already_created(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(
        state=State.PENDING,
        action="action",
        backend="test",
        commit="aaaaaaaaaabbbbbbbbbb11111111112222222222",
    )
    rap_request_body = rap_api_v1_factory_raw(
        backend="test",
        rap_id=job.job_request_id,
        commit=job.commit,
        repo_url=job.repo_url,
        workspace=job.workspace,
    )
    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200, response.json()

    response_json = response.json()
    assert response_json == {
        "result": "No change",
        "details": f"Jobs already created for rap_id '{job.job_request_id}'",
        "rap_id": job.job_request_id,
        "count": 1,
    }


def test_create_view_all_actions_already_scheduled(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    # create an existing pending job
    job = job_factory(
        repo_url=str(FIXTURES_PATH / "git-repo"),
        state=State.PENDING,
        action="generate_dataset",
        backend="test",
        commit="d090466f63b0d68084144d8f105f0d6e79a0819e",
    )

    # attempt to create a new job (with a different rap id) for the same action
    rap_request_body = rap_api_v1_factory_raw(
        backend="test",
        commit=job.commit,
        branch="v1",
        repo_url=job.repo_url,
        workspace=job.workspace,
        requested_actions=["generate_dataset"],
    )
    assert rap_request_body["rap_id"] != job.job_request_id

    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200, response.json()

    response_json = response.json()
    assert response_json == {
        "result": "Nothing to do",
        "details": "All requested actions were already scheduled to run",
        "rap_id": rap_request_body["rap_id"],
        "count": 0,
    }


def test_create_view_all_actions_already_run(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    repo_url = str(FIXTURES_PATH / "git-repo")
    workspace = "workspace"
    commit = "d090466f63b0d68084144d8f105f0d6e79a0819e"

    # create an existing successful job for each action
    project_file = read_file_from_repo(repo_url, commit, "project.yaml")
    pipeline_config = load_pipeline(project_file)

    for action in pipeline_config.all_actions:
        job_factory(
            repo_url=repo_url,
            state=State.SUCCEEDED,
            action=action,
            backend="test",
            commit=commit,
        )

    # attempt to create a new job (with a different rap id) for the same action
    rap_request_body = rap_api_v1_factory_raw(
        backend="test",
        commit=commit,
        branch="v1",
        repo_url=repo_url,
        workspace=workspace,
        requested_actions=["run_all"],
    )

    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200, response.json()

    response_json = response.json()
    assert response_json == {
        "result": "Nothing to do",
        "details": "All actions have already completed successfully",
        "rap_id": rap_request_body["rap_id"],
        "count": 0,
    }


def test_create_view_inconsistent_jobs_already_created(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action", backend="test")
    rap_id = job.job_request_id
    rap_request_body = rap_api_v1_factory_raw(
        backend="test",
        workspace="another_workspace",
        repo_url="another_repo_url",
        rap_id=rap_id,
    )
    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400, response.json()

    response_json = response.json()
    assert response_json == {
        "error": "Inconsistent request data",
        "details": f"Jobs already created for rap_id '{rap_id}' are inconsistent with request data",
    }


def test_create_view_with_git_error(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    repo_url = str(FIXTURES_PATH / "git-repo")
    bad_commit = "0" * 40

    rap_request_body = rap_api_v1_factory_raw(
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit=bad_commit,
        branch="v1",
        requested_actions=["generate_dataset"],
    )

    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "Error creating jobs",
        "details": f"Error fetching commit {bad_commit} from {repo_url}",
    }, response


@patch("controller.webapp.views.rap_views.create_jobs", side_effect=Exception("unk"))
def test_create_view_unexpected_error(mock_create_jobs, db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    repo_url = str(FIXTURES_PATH / "git-repo")

    rap_request_body = rap_api_v1_factory_raw(
        repo_url=repo_url,
        # GIT_DIR=tests/fixtures/git-repo git rev-parse v1
        commit="d090466f63b0d68084144d8f105f0d6e79a0819e",
        branch="v1",
        requested_actions=["generate_dataset"],
    )

    response = client.post(
        reverse("create"),
        json.dumps(rap_request_body),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "Error creating jobs",
        "details": "Unknown error",
    }, response


def test_job_to_api_format_default(db):
    job = job_factory()

    json = job_to_api_format(job)

    assert json["action"] == "action_name"
    assert json["run_command"] == "python myscript.py"
    assert json["status"] == "pending"
    assert json["status_code"] == "created"
    assert json["metrics"] == {}
    assert json["requires_db"] is False


def test_job_to_api_format_null_status_message(db):
    job = job_factory(status_message=None)
    json = job_to_api_format(job)
    assert json["status_message"] == ""


@pytest.mark.parametrize("agent_results", [True, False])
def test_job_to_api_format_metrics(db, agent_results):
    job = job_factory(state=State.RUNNING, action="action1", backend="test")
    if agent_results:
        runjob_db_task_factory(
            job=job,
            agent_results={
                "job_metrics": {"test": 0.0},
            },
        )
    else:
        runjob_db_task_factory(
            job=job,
        )

    if agent_results:
        assert job_to_api_format(job)["metrics"]["test"] == 0.0
    else:
        assert job_to_api_format(job)["metrics"] == {}


@pytest.mark.parametrize(
    "state, status_code",
    [
        (State.PENDING, StatusCode.CREATED),
        (State.RUNNING, StatusCode.EXECUTING),
        (State.SUCCEEDED, StatusCode.SUCCEEDED),
        (State.FAILED, StatusCode.DEPENDENCY_FAILED),
    ],
)
def test_status_view(db, client, monkeypatch, state, status_code):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(
        state=state, status_code=status_code, action="action1", backend="test"
    )

    post_data = {"rap_ids": [job.job_request_id]}
    response = client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )

    assert response.status_code == 200

    expected_started_at = None
    if state not in [State.PENDING]:
        expected_started_at = job.started_at_isoformat
    assert response.json() == {
        "jobs": [
            {
                "action": "action1",
                "backend": "test",
                "completed_at": None,
                "created_at": job.created_at_isoformat,
                "identifier": job.id,
                "metrics": {},
                "rap_id": job.job_request_id,
                "requires_db": False,
                "run_command": "python myscript.py",
                "started_at": expected_started_at,
                "status": state.value,
                # TODO: weird that this doesn't change - test data issue?
                "status_code": status_code.value,
                "status_message": "",
                "trace_context": job.trace_context,
                "updated_at": job.updated_at_isoformat,
            }
        ],
        "unrecognised_rap_ids": [],
    }, response


def test_status_view_validation_error(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    post_data = {}
    response = client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 400
    response_json = response.json()
    assert response_json == {
        "error": "Validation error",
        "details": "Invalid request body received: 'rap_ids' is a required property",
    }, response


def test_status_view_no_jobs_for_rap_id(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    post_data = {"rap_ids": ["hgfedca987654321"]}
    response = client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        "jobs": [],
        "unrecognised_rap_ids": ["hgfedca987654321"],
    }, response


def test_status_view_not_allowed_for_backend(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}

    job = job_factory(state=State.PENDING, action="action1", backend="foo")

    post_data = {"rap_ids": [job.job_request_id]}
    response = client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        "jobs": [],
        "unrecognised_rap_ids": [job.job_request_id],
    }, response


def test_status_view_tracing(db, client, monkeypatch):
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}
    setup_auto_tracing()

    job = job_factory(action="action1", backend="test")

    post_data = {"rap_ids": [job.job_request_id]}
    client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )

    traces = get_trace()
    last_trace = traces[-1]
    # default django attributes
    assert last_trace.attributes["http.request.method"] == "POST"
    assert last_trace.attributes["http.route"] == ("controller/v1/rap/status/")
    assert last_trace.attributes["http.response.status_code"] == 200

    assert last_trace.attributes["valid_rap_ids"] == job.job_request_id
    assert last_trace.attributes["unrecognised_rap_ids"] == ""
    assert last_trace.attributes["extra_rap_ids"] == ""
    # Duration is rounded so will be 0 in the test because it's so quick. This is
    # tested elsewhere, so we just test that the key is correctly added to the traced
    # attributes
    assert "find_matching_jobs.duration_ms" in last_trace.attributes
    assert "find_extra_rap_ids.duration_ms" in last_trace.attributes


def test_status_view_tracing_with_unexpected_rap_ids(db, client, monkeypatch):
    monkeypatch.setattr("common.config.BACKENDS", ["test", "test1"])
    monkeypatch.setattr("controller.config.CLIENT_TOKENS", {"test_token": ["test"]})
    headers = {"Authorization": "test_token"}
    setup_auto_tracing()

    job1 = job_factory(action="action1", backend="test")
    job2 = job_factory(action="action1", backend="test")
    job3 = job_factory(action="action1", backend="test1")
    job_factory(action="action1", backend="test1")

    # All 3 jobs have different rap ids
    assert len({j.job_request_id for j in [job1, job2, job3]}) == 3

    # job1 is active and requested
    # job2 is active but not requested
    # job3 is active and requested but the client token doesn't have access to its backend
    # last job is active and not requested but the client token doesn't have access to its backend

    # Request job1 and job3's job request ids, and one that doesn't exist at all
    post_data = {
        "rap_ids": [job1.job_request_id, job3.job_request_id, "unknown123456789"]
    }
    client.post(
        reverse("status"),
        json.dumps(post_data),
        headers=headers,
        content_type="application/json",
    )

    traces = get_trace()
    last_trace = traces[-1]
    # default django attributes
    assert last_trace.attributes["http.request.method"] == "POST"
    assert last_trace.attributes["http.route"] == ("controller/v1/rap/status/")
    assert last_trace.attributes["http.response.status_code"] == 200

    assert last_trace.attributes["valid_rap_ids"] == job1.job_request_id
    # unrecognised rap IDs are ones that we requested, but aren't valid, either because there are no
    # matching jobs at all, or because jobs aren't found for backends that the client token has access to
    assert set(last_trace.attributes["unrecognised_rap_ids"].split(",")) == {
        job3.job_request_id,
        "unknown123456789",
    }
    # extra rap ids are for jobs that are active and for backends that the token has access to
    # but have rap ids that we did NOT request
    assert last_trace.attributes["extra_rap_ids"] == job2.job_request_id
    # Duration is rounded so will be 0 in the test because it's so quick. This is
    # tested elsewhere, so we just test that the key is correctly added to the traced
    # attributes
    assert "find_matching_jobs.duration_ms" in last_trace.attributes
    assert "find_extra_rap_ids.duration_ms" in last_trace.attributes
