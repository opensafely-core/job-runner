import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from jobrunner import config
from jobrunner.job_executor import Study
from jobrunner.lib import database
from jobrunner.lib.subprocess_utils import subprocess_run


def pytest_configure(config):
    config.addinivalue_line("markers", "slow_test: mark test as being slow running")
    config.addinivalue_line(
        "markers", "needs_docker: mark test as needing Docker daemon"
    )


@pytest.fixture
def tmp_work_dir(monkeypatch, tmp_path):
    monkeypatch.setattr("jobrunner.config.WORKDIR", tmp_path)
    monkeypatch.setattr("jobrunner.config.DATABASE_FILE", tmp_path / "db.sqlite")
    config_vars = [
        "TMP_DIR",
        "GIT_REPO_DIR",
        "HIGH_PRIVACY_STORAGE_BASE",
        "MEDIUM_PRIVACY_STORAGE_BASE",
        "HIGH_PRIVACY_WORKSPACES_DIR",
        "MEDIUM_PRIVACY_WORKSPACES_DIR",
        "HIGH_PRIVACY_ARCHIVE_DIR",
        "JOB_LOG_DIR",
    ]
    for config_var in config_vars:
        monkeypatch.setattr(
            f"jobrunner.config.{config_var}", tmp_path / config_var.lower()
        )
    return tmp_path


@pytest.fixture
def docker_cleanup(monkeypatch):
    label_for_tests = "jobrunner-pytest"
    monkeypatch.setattr("jobrunner.lib.docker.LABEL", label_for_tests)
    monkeypatch.setattr("jobrunner.executors.local.LABEL", label_for_tests)
    yield
    delete_docker_entities("container", label_for_tests)
    delete_docker_entities("volume", label_for_tests)


def delete_docker_entities(entity, label, ignore_errors=False):
    ls_args = [
        "docker",
        entity,
        "ls",
        "--all" if entity == "container" else None,
        "--filter",
        f"label={label}",
        "--quiet",
    ]
    ls_args = list(filter(None, ls_args))
    response = subprocess.run(
        ls_args, capture_output=True, encoding="ascii", check=not ignore_errors
    )
    ids = response.stdout.split()
    if ids and response.returncode == 0:
        rm_args = ["docker", entity, "rm", "--force"] + ids
        subprocess.run(rm_args, capture_output=True, check=not ignore_errors)


@dataclass
class TestRepo:
    source: str
    path: str
    commit: str
    study: Study


@pytest.fixture
def test_repo(tmp_work_dir):
    """Take our test project fixture and commit it to a temporary git repo"""
    directory = Path(__file__).parent.resolve() / "fixtures/full_project"
    repo_path = tmp_work_dir / "test-repo"

    env = {"GIT_WORK_TREE": str(directory), "GIT_DIR": repo_path}
    subprocess_run(["git", "init", "--bare", "--quiet", repo_path], check=True)
    subprocess_run(
        ["git", "config", "user.email", "test@example.com"], check=True, env=env
    )
    subprocess_run(["git", "config", "user.name", "Test"], check=True, env=env)
    subprocess_run(["git", "add", "."], check=True, env=env)
    subprocess_run(["git", "commit", "--quiet", "-m", "initial"], check=True, env=env)
    response = subprocess_run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        env=env,
        capture_output=True,
        text=True,
    )
    commit = response.stdout.strip()
    return TestRepo(
        source=directory,
        path=repo_path,
        commit=commit,
        study=Study(git_repo_url=str(repo_path), commit=commit),
    )


@pytest.fixture()
def db(monkeypatch):
    """Create a throwaway db."""
    database_file = ":memory:{random.randrange(sys.maxsize)}"
    monkeypatch.setattr(config, "DATABASE_FILE", database_file)
    yield
    del database.CONNECTION_CACHE.__dict__[database_file]
