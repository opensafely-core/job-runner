import pytest


@pytest.fixture(autouse=True)
def test_backend_api_tokens(monkeypatch):
    """
    Ensures that the agent and controller both have the relevant tokens
    set for the agent to authenticate with the controller app
    """
    monkeypatch.setattr("agent.config.TASK_API_TOKEN", "test_token")
    monkeypatch.setattr("controller.config.JOB_SERVER_TOKENS", {"test": "test_token"})
