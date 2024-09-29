import json

from responses import matchers

from jobrunner import config, queries, sync
from jobrunner.lib.database import find_where
from jobrunner.models import Job, JobRequest
from tests.factories import job_factory, metrics_factory


def test_job_request_from_remote_format():
    remote_job_request = {
        "identifier": "123",
        "workspace": {
            "name": "testing",
            "repo": "https://github.com/opensafely/foo",
            "branch": "master",
        },
        "database_name": "default",
        "requested_actions": ["generate_cohort"],
        "cancelled_actions": ["analyse"],
        "force_run_dependencies": True,
        "sha": "abcdef",
        "codelists_ok": True,
        "created_by": "user",
        "project": "project",
        "orgs": ["org"],
    }
    expected = JobRequest(
        id="123",
        repo_url="https://github.com/opensafely/foo",
        commit="abcdef",
        branch="master",
        workspace="testing",
        codelists_ok=True,
        database_name="default",
        requested_actions=["generate_cohort"],
        cancelled_actions=["analyse"],
        force_run_dependencies=True,
        original=remote_job_request,
    )
    job_request = sync.job_request_from_remote_format(remote_job_request)
    assert job_request == expected


def test_job_request_from_remote_format_database_name_fallback():
    remote_job_request = {
        "identifier": "123",
        "workspace": {
            "name": "testing",
            "repo": "https://github.com/opensafely/foo",
            "branch": "master",
            "db": "default",
        },
        "requested_actions": ["generate_cohort"],
        "cancelled_actions": ["analyse"],
        "force_run_dependencies": True,
        "sha": "abcdef",
        "codelists_ok": True,
        "created_by": "user",
        "project": "project",
        "orgs": ["org"],
    }
    expected = JobRequest(
        id="123",
        repo_url="https://github.com/opensafely/foo",
        commit="abcdef",
        branch="master",
        workspace="testing",
        codelists_ok=True,
        database_name="default",
        requested_actions=["generate_cohort"],
        cancelled_actions=["analyse"],
        force_run_dependencies=True,
        original=remote_job_request,
    )
    job_request = sync.job_request_from_remote_format(remote_job_request)
    assert job_request == expected


def test_job_to_remote_format_default(db):
    job = job_factory()

    json = sync.job_to_remote_format(job)

    assert json["action"] == "action_name"
    assert json["run_command"] == "python myscript.py"
    assert json["status"] == "pending"
    assert json["status_code"] == "created"
    assert json["metrics"] == {}
    assert json["requires_db"] is False


def test_job_to_remote_format_null_status_message(db):
    job = job_factory(status_message=None)
    json = sync.job_to_remote_format(job)
    assert json["status_message"] == ""


def test_job_to_remote_format_metrics(db):
    job = job_factory()
    metrics_factory(job, metrics={"test": 0.0})

    json = sync.job_to_remote_format(job)

    assert json["metrics"] == {"test": 0.0}


def test_session_request_no_flags(db, responses):
    responses.add(
        method="GET",
        url=f"{config.JOB_SERVER_ENDPOINT}path/?backend=test",
        status=200,
        json="{}",
        match=[
            matchers.header_matcher(
                {
                    "Authorization": config.JOB_SERVER_TOKEN,
                    "Flags": "{}",
                }
            ),
        ],
    )

    # if this works, our expected request was generated
    sync.api_get("path", params={"backend": "test"})


def test_session_request_flags(db, responses):
    f1 = queries.set_flag("mode", "db-maintenance")
    f2 = queries.set_flag("pause", "true")

    flags_dict = {
        "mode": {"v": "db-maintenance", "ts": f1.timestamp_isoformat},
        "pause": {"v": "true", "ts": f2.timestamp_isoformat},
    }
    expected_header = json.dumps(flags_dict, separators=(",", ":"))

    responses.add(
        method="GET",
        url=f"{config.JOB_SERVER_ENDPOINT}path/?backend=test",
        status=200,
        json="{}",
        match=[
            matchers.header_matcher(
                {
                    "Authorization": config.JOB_SERVER_TOKEN,
                    "Flags": expected_header,
                }
            ),
        ],
    )

    # if this works, our expected request was generated
    sync.api_get("path", params={"backend": "test"})


def test_sync_empty_response(db, monkeypatch, requests_mock):
    monkeypatch.setattr(
        "jobrunner.config.JOB_SERVER_ENDPOINT", "http://testserver/api/v2/"
    )
    requests_mock.get(
        "http://testserver/api/v2/job-requests/?backend=expectations",
        json={
            "results": [],
        },
    )
    sync.sync()

    # verify we did not post back to job-server
    assert requests_mock.last_request.text is None
    assert requests_mock.last_request.method == "GET"

    # also that we did not create any jobs
    jobs = find_where(Job)
    assert jobs == []
