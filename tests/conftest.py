import pytest


@pytest.fixture(autouse=True)
def set_environ(monkeypatch):
    monkeypatch.setenv("QUEUE_USER", "")
    monkeypatch.setenv("QUEUE_PASS", "")
    monkeypatch.setenv("OPENSAFELY_RUNNER_STORAGE_BASE", "")
    monkeypatch.setenv("FULL_DATABASE_URL", "sqlite:///test.db")
