import subprocess

import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "slow_test: mark test as being slow running")
    config.addinivalue_line(
        "markers", "needs_docker: mark test as needing Docker daemon"
    )


@pytest.fixture
def tmp_work_dir(monkeypatch, tmp_path):
    monkeypatch.setattr("jobrunner.config.WORK_DIR", tmp_path)
    monkeypatch.setattr("jobrunner.config.DATABASE_FILE", tmp_path / "db.sqlite")
    config_vars = [
        "TMP_DIR",
        "GIT_REPO_DIR",
        "HIGH_PRIVACY_STORAGE_BASE",
        "MEDIUM_PRIVACY_STORAGE_BASE",
        "HIGH_PRIVACY_WORKSPACES_DIR",
        "MEDIUM_PRIVACY_WORKSPACES_DIR",
        "JOB_LOG_DIR",
    ]
    for config_var in config_vars:
        monkeypatch.setattr(
            f"jobrunner.config.{config_var}", tmp_path / config_var.lower()
        )
    return tmp_path


@pytest.fixture(scope="module")
def docker_cleanup():
    # Workaround for the fact that `monkeypatch` is only function-scoped.
    # Hopefully will be unnecessary soon. See:
    # https://github.com/pytest-dev/pytest/issues/363
    from _pytest.monkeypatch import MonkeyPatch

    label_for_tests = "jobrunner-test-R5o1iLu"
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr("jobrunner.lib.docker.LABEL", label_for_tests)
    yield
    delete_docker_entities("container", label_for_tests)
    delete_docker_entities("volume", label_for_tests)
    monkeypatch.undo()


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
