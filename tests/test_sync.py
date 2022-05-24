import json

from responses import matchers

from jobrunner import config, queries, sync
from jobrunner.models import JobRequest


def test_job_request_from_remote_format():
    remote_job_request = {
        "identifier": "123",
        "workspace": {
            "name": "testing",
            "repo": "https://github.com/opensafely/foo",
            "branch": "master",
            "db": "full",
        },
        "requested_actions": ["generate_cohort"],
        "cancelled_actions": ["analyse"],
        "force_run_dependencies": True,
        "sha": "abcdef",
    }
    expected = JobRequest(
        id="123",
        repo_url="https://github.com/opensafely/foo",
        commit="abcdef",
        branch="master",
        workspace="testing",
        database_name="full",
        requested_actions=["generate_cohort"],
        cancelled_actions=["analyse"],
        force_run_dependencies=True,
        original=remote_job_request,
    )
    job_request = sync.job_request_from_remote_format(remote_job_request)
    assert job_request == expected


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
