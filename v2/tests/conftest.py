import pytest


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
