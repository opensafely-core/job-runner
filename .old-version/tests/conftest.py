import os
import tempfile

import pytest


@pytest.fixture(autouse=True)
def set_environ(monkeypatch):
    monkeypatch.setenv("QUEUE_USER", "")
    monkeypatch.setenv("QUEUE_PASS", "")
    monkeypatch.setenv("OPENSAFELY_RUNNER_STORAGE_BASE", "")
    monkeypatch.setenv("FULL_DATABASE_URL", "sqlite:///test.db")
    monkeypatch.setenv("TEMP_DATABASE_NAME", "temp")
    monkeypatch.setenv("BACKEND", "tpp")
    monkeypatch.setenv("JOB_SERVER_ENDPOINT", "http://test.com/jobs/")
    storage_root = tempfile.TemporaryDirectory().name
    high_privacy_storage_base = os.path.join(storage_root, "highsecurity")
    medium_privacy_storage_base = os.path.join(storage_root, "mediumsecurity")
    monkeypatch.setenv("HIGH_PRIVACY_STORAGE_BASE", high_privacy_storage_base)
    monkeypatch.setenv("MEDIUM_PRIVACY_STORAGE_BASE", medium_privacy_storage_base)
    os.makedirs(high_privacy_storage_base, exist_ok=True)
    os.makedirs(medium_privacy_storage_base, exist_ok=True)


@pytest.fixture(scope="function")
def workspace():
    return {
        "repo": "https://github.com/repo",
        "db": "full",
        "owner": "me",
        "name": "tofu",
        "branch": "master",
        "id": 1,
    }


@pytest.fixture(scope="function")
def job_spec_maker(workspace):
    def _job_spec(**kw):
        default = {
            "action_id": "",
            "force_run": False,
            "pk": 0,
            "force_run_dependencies": False,
            "backend": "tpp",
            "workspace": workspace,
            "workspace_id": workspace["id"],
        }
        default.update(kw)
        return default

    return _job_spec


@pytest.fixture(scope="function")
def prepared_job_maker(job_spec_maker):
    def _prepared_job(**kw):
        job_spec = job_spec_maker()
        job_spec.update(
            {
                "container_name": "docker-container",
                "action_id": "run_model",
                "docker_invocation": ["docker-invocation"],
            }
        )
        job_spec.update(kw)
        return job_spec

    return _prepared_job
