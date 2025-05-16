from unittest import mock

import pytest

from jobrunner.cli.agent import kill_task


@pytest.fixture(autouse=True)
def mock_delete_volume():
    with mock.patch("jobrunner.cli.agent.kill_task.delete_volume", autospec=True):
        yield


@pytest.fixture
def mock_container_ls():
    with mock.patch(
        "jobrunner.cli.agent.kill_task.docker", autospec=True
    ) as mock_docker:
        mock_docker.docker.return_value = mock.Mock(
            stdout="'\"os-job-1234\"'\n'\"os-job-1245\"'\n"
        )
        yield


def test_get_container_names(mock_container_ls):
    assert kill_task.get_container_names() == ["os-job-1234", "os-job-1245"]


@mock.patch("jobrunner.cli.agent.kill_task.docker", autospec=True)
def test_get_container_names_no_containers(mock_docker):
    mock_docker.docker.return_value = mock.Mock(stdout="")
    assert kill_task.get_container_names() == []


def test_get_job_containers_no_matches(mock_container_ls):
    partial_job_ids = ["1256"]
    kill_task.get_job_containers(partial_job_ids) == []


def test_get_job_containers_with_full_match(mock_container_ls):
    partial_job_ids = ["1234"]
    kill_task.get_job_containers(partial_job_ids) == ["os-job-1234"]


def test_get_job_containers_with_partial_match(mock_container_ls, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "")
    partial_job_ids = ["123"]
    kill_task.get_job_containers(partial_job_ids) == ["os-job-1234"]


def test_get_job_multiple_matches(mock_container_ls, monkeypatch):
    partial_job_ids = ["12"]
    monkeypatch.setattr("builtins.input", lambda _: "1")
    kill_task.get_job_containers(partial_job_ids) == ["os-job-1234"]


def test_main_no_running_container(monkeypatch, capsys):
    monkeypatch.setattr("builtins.input", lambda _: "")
    with mock.patch(
        "jobrunner.cli.agent.kill_task.docker", autospec=True
    ) as mock_docker:
        mock_docker.docker.return_value = mock.Mock(stdout="'\"os-job-1234\"'\n")
        mock_docker.container_exists.return_value = False
        kill_task.main(["1234"])

    assert "Cannot kill task for job 1234" in capsys.readouterr().out
    mock_docker.kill.assert_not_called


def test_main_kill_one_task(monkeypatch, capsys):
    monkeypatch.setattr("builtins.input", lambda _: "")
    with mock.patch(
        "jobrunner.cli.agent.kill_task.docker", autospec=True
    ) as mock_docker:
        mock_docker.docker.return_value = mock.Mock(stdout="'\"os-job-1234\"'\n")
        mock_docker.container_exists.return_value = True
        kill_task.main(["1234"])
    assert "Task for job 1234 killed" in capsys.readouterr().out
    mock_docker.kill.assert_called_with("os-job-1234")
    mock_docker.delete_container.assert_called_with("os-job-1234")


def test_main_kill_multiple_tasks(monkeypatch, capsys):
    monkeypatch.setattr("builtins.input", lambda _: "")
    with mock.patch(
        "jobrunner.cli.agent.kill_task.docker", autospec=True
    ) as mock_docker:
        mock_docker.docker.return_value = mock.Mock(
            stdout="'\"os-job-1234\"'\n'\"os-job-1245\"'\n"
        )
        mock_docker.container_exists.return_value = True
        kill_task.main(["1234", "1245", "unk"])
    out = capsys.readouterr().out
    assert "Task for job 1234 killed" in out
    assert "Task for job 1245 killed" in out
    assert "No running tasks found matching 'unk'" in out
    assert [call.args for call in mock_docker.kill.call_args_list] == [
        ("os-job-1234",),
        ("os-job-1245",),
    ]


def test_run(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _: "")
    with mock.patch(
        "jobrunner.cli.agent.kill_task.docker", autospec=True
    ) as mock_docker:
        mock_docker.docker.return_value = mock.Mock(stdout="'\"os-job-1234\"'\n")
        mock_docker.container_exists.return_value = True
        kill_task.run(["kill_task", "1234"])
