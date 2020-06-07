import pytest


@pytest.fixture(autouse=True)
def set_environ(monkeypatch):
    monkeypatch.setenv("QUEUE_USER", "")
    monkeypatch.setenv("QUEUE_PASS", "")
