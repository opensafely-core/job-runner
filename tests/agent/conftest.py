import pytest


@pytest.fixture(autouse=True)
def test_backend_api_tokens(monkeypatch):
    """
    Ensures that the agent and controller both have the relevant tokens
    set for the agent to authenticate with the controller app
    """
    monkeypatch.setattr("jobrunner.config.agent.TASK_API_TOKEN", "test_token")
    monkeypatch.setattr(
        "jobrunner.config.controller.JOB_SERVER_TOKENS", {"test": "test_token"}
    )
