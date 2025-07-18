import ast
import os
import subprocess
import sys
import tempfile
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from unittest import mock

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent import config as agent_config
from agent import metrics
from common.job_executor import Study
from common.tracing import add_exporter, get_provider
from controller import config as controller_config
from controller.lib import database


# set up test tracing
provider = get_provider("test")
trace.set_tracer_provider(provider)
test_exporter = InMemorySpanExporter()
add_exporter(provider, test_exporter, processor=SimpleSpanProcessor)


def pytest_configure(config):
    config.addinivalue_line("markers", "slow_test: mark test as being slow running")
    config.addinivalue_line(
        "markers", "needs_docker: mark test as needing Docker daemon"
    )


@pytest.fixture(autouse=True)
def clear_state():
    yield
    database.CONNECTION_CACHE.__dict__.clear()
    # clear any exported spans
    test_exporter.clear()


def get_trace(tracer=None):
    spans = test_exporter.get_finished_spans()
    if tracer is None:
        return spans
    else:
        return [s for s in spans if s.instrumentation_scope.name == tracer]


def set_tmp_workdir_config(monkeypatch, tmp_path):
    monkeypatch.setattr("common.config.WORKDIR", tmp_path)
    monkeypatch.setattr("controller.config.DATABASE_FILE", tmp_path / "db.sqlite")
    config_vars = {
        "common": ["GIT_REPO_DIR"],
        "agent": [
            "TMP_DIR",
            "HIGH_PRIVACY_STORAGE_BASE",
            "MEDIUM_PRIVACY_STORAGE_BASE",
            "HIGH_PRIVACY_WORKSPACES_DIR",
            "MEDIUM_PRIVACY_WORKSPACES_DIR",
            "HIGH_PRIVACY_ARCHIVE_DIR",
            "HIGH_PRIVACY_VOLUME_DIR",
            "JOB_LOG_DIR",
        ],
    }
    for config_type, conf_vars in config_vars.items():
        for var in conf_vars:
            monkeypatch.setattr(f"{config_type}.config.{var}", tmp_path / var.lower())


@pytest.fixture
def tmp_work_dir(request, monkeypatch, tmp_path):
    set_tmp_workdir_config(monkeypatch, tmp_path)

    # ensure db initialise
    database.ensure_db()

    # Ok, so this is a bit complex.
    #
    # For running the tests in a docker container, we need to make pytest's
    # tmp_path readable by the host. This is so the host's docker service
    # (which the jobrunner-inna-container uses to run jobs) can bind mount the
    # HIGH_PRIVACY_VOLUME_DIR into the job container.
    #
    # We do this via:
    #
    # a) mounting a host directory into the container as /tmp
    # b) letting pytest do it's thing in /tmp
    # c) calculate and set DOCKER_HOST_VOLUME_DIR to point the the host's view
    # of each test's individual temp dirs
    #
    # This pretty much amounts to replacing /tmp/ with PYTEST_HOST_TMP, but we
    # make a best effort attempt to support pytest's --basetemp config.
    #
    # Note 1: for this to work, it requires cooperation with the values in
    # docker-compose.yml, the PYTEST_HOST_TMP var needs to a) exist b) be owned
    # by the user and c) be mounted as /tmp inside the container.
    #
    # Note 2: ideally, we would force pytest basedir to be /tmp, to avoid
    # breaking the coupling between pytest config and docker-compose config.
    # Technically, we can actually do this via the undocumented env var
    # PYTEST_DEBUG_TEMPROOT[1], but that feels a bit icky. But it would
    # probably reduce the chances of developers accidentaly breaking things.
    #
    # [1]https://github.com/pytest-dev/pytest/blob/main/src/_pytest/tmpdir.py#L114
    pytest_host_tmp = os.environ.get("PYTEST_HOST_TMP")
    if pytest_host_tmp:
        # attempt to handle --basetemp cli arg being used to change the default
        # location
        basetemp = (
            request.config.option.basetemp or Path(tempfile.gettempdir()).resolve()
        )
        host_volume_path = pytest_host_tmp / tmp_path.relative_to(basetemp)
        monkeypatch.setattr(
            "agent.config.DOCKER_HOST_VOLUME_DIR",
            host_volume_path / "high_privacy_volume_dir".lower(),
        )

    return tmp_path


@pytest.fixture
def docker_cleanup(monkeypatch):
    label_for_tests = "jobrunner-pytest"
    monkeypatch.setattr("agent.lib.docker.LABEL", label_for_tests)
    monkeypatch.setattr("agent.executors.local.LABEL", label_for_tests)
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
    subprocess.run(
        ["git", "init", "--bare", "--initial-branch=main", "--quiet", repo_path],
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"], check=True, env=env
    )
    subprocess.run(["git", "config", "user.name", "Test"], check=True, env=env)
    subprocess.run(["git", "add", "."], check=True, env=env)
    subprocess.run(["git", "commit", "--quiet", "-m", "initial"], check=True, env=env)
    response = subprocess.run(
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


@pytest.fixture(autouse=True)
def db_config(monkeypatch):
    """
    Always patch DATABASE_FILE to None to ensure the db fixture
    has been used for tests that need to use the db.
    """
    monkeypatch.setattr(controller_config, "DATABASE_FILE", None)


@pytest.fixture()
def db(monkeypatch, request):
    """Create a throwaway db."""
    database_file = f"file:db-{request.node.name}?mode=memory&cache=shared"
    monkeypatch.setattr(controller_config, "DATABASE_FILE", database_file)
    database.ensure_db(controller_config.DATABASE_FILE)
    yield
    del database.CONNECTION_CACHE.__dict__[controller_config.DATABASE_FILE]


@pytest.fixture(autouse=True)
def metrics_db(monkeypatch, tmp_path, request):
    """Create a throwaway metrics db.

    It must be a file, not memory, because we use readonly connections.
    """
    db_path = tmp_path / "metrics.db"
    monkeypatch.setattr(agent_config, "METRICS_FILE", db_path)
    yield
    metrics.CONNECTION_CACHE.__dict__.clear()


@dataclass
class SubprocessStub:
    calls: deque = field(default_factory=deque)

    def add_call(self, cmd, **kwargs):
        ps = subprocess.CompletedProcess(cmd, returncode=0)
        self.calls.append((cmd, kwargs, ps))
        # caller can alter to match desired behaviour
        return ps

    def run(self, call_args, **call_kwargs):
        args, kwargs, ps = self.calls.popleft()
        assert call_args == args, f"subprocess.run expected {args}, got {call_args}"
        assert call_kwargs == kwargs, (
            f"subprocess.run expected kwargs {kwargs}, got {call_kwargs}"
        )
        if ps.returncode != 0 and kwargs.get("check"):
            raise subprocess.CalledProcessError(
                args, ps.returncode, ps.stdout, ps.stderr
            )
        return ps


@pytest.fixture
def mock_subprocess_run():
    stub = SubprocessStub()
    with mock.patch("subprocess.run", stub.run):
        yield stub
    assert len(stub.calls) == 0, (
        f"subprocess.run expected the following calls: {stub.calls}"
    )


def import_cfg(env, script, raises=None):
    try:
        ps = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            text=True,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as err:
        print(err.stderr)
        raise

    print(ps.stdout)
    return ast.literal_eval(ps.stdout)
