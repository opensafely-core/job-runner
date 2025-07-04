import os
from pathlib import Path
from subprocess import CalledProcessError
from unittest import mock

import pytest

from jobrunner.lib.git import (
    GitError,
    GitFileNotFoundError,
    GitRepoNotReachableError,
    GitUnknownRefError,
    add_access_token_and_proxy,
    checkout_commit,
    commit_already_fetched,
    commit_reachable_from_ref,
    ensure_git_init,
    fetch_commit,
    get_sha_from_remote_ref,
    read_file_from_repo,
    redact_token_from_exception,
)


REPO_FIXTURE = str(Path(__file__).parents[1].resolve() / "fixtures/git-repo")


@pytest.mark.slow_test
def test_read_file_from_repo(tmp_work_dir):
    output = read_file_from_repo(
        "https://github.com/opensafely-core/test-public-repository.git",
        "c1ef0e676ec448b0a49e0073db364f36f6d6d078",
        "README.md",
    )
    assert output == b"# test-public-repository"


@pytest.mark.slow_test
def test_checkout_commit(tmp_work_dir, tmp_path):
    target_dir = tmp_path / "files"
    checkout_commit(
        "https://github.com/opensafely-core/test-public-repository.git",
        "c1ef0e676ec448b0a49e0073db364f36f6d6d078",
        target_dir,
    )
    assert [f.name for f in target_dir.iterdir()] == ["README.md"]


@pytest.mark.slow_test
def test_get_sha_from_remote_ref(tmp_work_dir):
    sha = get_sha_from_remote_ref(
        "https://github.com/opensafely-core/test-public-repository.git",
        "test-tag-dont-delete",
    )
    assert sha == "029a6ff81cb0ab878de24c12bc690969163c5c9e"


@pytest.mark.slow_test
def test_get_sha_from_remote_ref_annotated_tag():
    sha = get_sha_from_remote_ref(
        "https://github.com/opensafely-core/test-public-repository.git",
        "test-annotated-tag-dont-delete",
    )
    assert sha == "3c15ff525001e039d4e27cfc62f652ecad09fde4"


@pytest.mark.slow_test
def test_get_sha_from_remote_ref_missing_ref(tmp_work_dir):
    with pytest.raises(GitUnknownRefError):
        get_sha_from_remote_ref(
            "https://github.com/opensafely-core/test-public-repository.git",
            "no-such-ref",
        )


@pytest.mark.slow_test
def test_get_sha_from_remote_ref_missing_repo(tmp_work_dir):
    with pytest.raises(GitRepoNotReachableError):
        get_sha_from_remote_ref(
            "https://github.com/opensafely-core/no-such-repo.git", "main"
        )


@pytest.mark.slow_test
def test_commit_reachable_from_ref(tmp_work_dir):
    is_reachable_good = commit_reachable_from_ref(
        "https://github.com/opensafely-core/test-public-repository.git",
        "029a6ff81cb0ab878de24c12bc690969163c5c9e",
        "test-branch-dont-delete",
    )
    assert is_reachable_good
    is_reachable_bad = commit_reachable_from_ref(
        "https://github.com/opensafely-core/test-public-repository.git",
        "029a6ff81cb0ab878de24c12bc690969163c5c9e",
        "main",
    )
    assert not is_reachable_bad


# These tests makes request to an actual private GitHub repo and so will only
# work if there's an appropriate access token in the environment


@pytest.mark.skipif(
    not os.environ.get("PRIVATE_REPO_ACCESS_TOKEN"),
    reason="No access token in environment",
)
@pytest.mark.slow_test
def test_read_file_from_private_repo(tmp_work_dir):
    output = read_file_from_repo(
        "https://github.com/opensafely/test-repository.git",
        "d7fe87ab5d6dc97222c4a9dbf7c0fe40fc108c8f",
        "README.md",
    )
    assert output == b"# test-repository\nTesting GH permssions model\n"


@pytest.mark.skipif(
    not os.environ.get("PRIVATE_REPO_ACCESS_TOKEN"),
    reason="No access token in environment",
)
@pytest.mark.slow_test
def test_get_sha_from_remote_ref_private(tmp_work_dir):
    sha = get_sha_from_remote_ref(
        "https://github.com/opensafely/test-repository", "v1.0"
    )
    assert sha == "981ac62ec5620df90556bc18784f06b6e7db7e4d"


# The below tests use a local git repo fixture rather than accessing GitHub
# over HTTPS. This makes them faster, though obviously less complete.


def test_read_file_from_repo_local(tmp_work_dir):
    output = read_file_from_repo(
        REPO_FIXTURE,
        "cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74",
        "project.yaml",
    )
    assert output.startswith(b"version: '1.0'")


def test_read_file_from_repo_local_does_not_exist(tmp_work_dir):
    with pytest.raises(GitFileNotFoundError, match="File 'unknown.yaml' not found"):
        read_file_from_repo(
            REPO_FIXTURE,
            "cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74",
            "unknown.yaml",
        )


def test_checkout_commit_local(tmp_work_dir, tmp_path):
    target_dir = tmp_path / "files"
    checkout_commit(
        REPO_FIXTURE,
        "cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74",
        target_dir,
    )
    assert [f.name for f in target_dir.iterdir()] == ["project.yaml"]


def test_get_sha_from_remote_ref_local(tmp_work_dir):
    sha = get_sha_from_remote_ref(REPO_FIXTURE, "v1")
    assert sha == "cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74"


def test_get_sha_from_remote_ref_local_missing_ref(tmp_work_dir):
    with pytest.raises(GitUnknownRefError):
        get_sha_from_remote_ref(REPO_FIXTURE, "no-such-ref")


def test_get_sha_from_remote_ref_local_missing_repo(tmp_work_dir):
    MISSING_REPO = REPO_FIXTURE + "-no-such-repo"
    with pytest.raises(GitRepoNotReachableError):
        get_sha_from_remote_ref(MISSING_REPO, "v1")


def test_commit_already_fetched(tmp_path):
    commit_sha = "cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74"
    repo_dir = tmp_path / "repo"
    ensure_git_init(repo_dir)
    assert not commit_already_fetched(repo_dir, commit_sha)
    fetch_commit(repo_dir, REPO_FIXTURE, commit_sha)
    assert commit_already_fetched(repo_dir, commit_sha)


@mock.patch("jobrunner.lib.git.time.sleep")
def test_commit_fetch_retry(mock_sleep, tmp_path):
    commit_sha = "cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74"
    repo_dir = tmp_path / "repo"
    ensure_git_init(repo_dir)
    assert not commit_already_fetched(repo_dir, commit_sha)

    with mock.patch(
        "jobrunner.lib.git.subprocess.run",
        side_effect=[
            # 5 retries are allowed; mock a caught exception for the first 4
            CalledProcessError(returncode=1, cmd="git", stderr=b"GnuTLS recv error"),
            CalledProcessError(
                returncode=1, cmd="git", stderr=b"SSL_read: Connection was reset"
            ),
            CalledProcessError(returncode=1, cmd="git", stderr=b"GnuTLS recv error"),
            CalledProcessError(
                returncode=1, cmd="git", stderr=b"SSL_read: Connection was reset"
            ),
            # git fetch succeeds
            None,
            # mark_commmit_as_fetched (git tag) succeeds
            None,
        ],
    ):
        fetch_commit(repo_dir, REPO_FIXTURE, commit_sha)


@mock.patch("jobrunner.lib.git.time.sleep")
def test_commit_fetch_retry_max_attempts(mock_sleep, tmp_path):
    commit_sha = "cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74"
    repo_dir = tmp_path / "repo"
    ensure_git_init(repo_dir)
    assert not commit_already_fetched(repo_dir, commit_sha)

    with mock.patch(
        "jobrunner.lib.git.subprocess.run",
        side_effect=[
            CalledProcessError(returncode=1, cmd="git", stderr=b"GnuTLS recv error"),
        ]
        * 5,
    ):
        with pytest.raises(
            GitError, match=f"Network error when fetching commit {commit_sha}"
        ):
            fetch_commit(repo_dir, REPO_FIXTURE, commit_sha)


def test_commit_fetch_retry_unexpected_error(tmp_path):
    commit_sha = "cfbd0fe545d4e4c0747f0746adaa79ce5f8dfc74"
    repo_dir = tmp_path / "repo"
    ensure_git_init(repo_dir)
    assert not commit_already_fetched(repo_dir, commit_sha)

    with mock.patch(
        "jobrunner.lib.git.subprocess.run",
        side_effect=[
            CalledProcessError(returncode=1, cmd="git", stderr=b"Unknown error"),
        ],
    ):
        with pytest.raises(GitError, match=f"Error fetching commit {commit_sha}"):
            fetch_commit(repo_dir, REPO_FIXTURE, commit_sha)


@pytest.mark.parametrize(
    "repo_url,token,backend,expected",
    [
        # uses agent BACKEND in username
        (
            "https://github.com/org/repo",
            "token",
            "test",
            "https://jobrunner-test:token@proxy.com/org/repo",
        ),
        # no BACKEND, uses ctrl (for controller) in username
        (
            "https://github.com/org/repo",
            "token",
            None,
            "https://jobrunner-ctrl:token@proxy.com/org/repo",
        ),
        # no token, return without username/pass
        ("https://github.com/org/repo", "", "test", "https://proxy.com/org/repo"),
        # no https, return without username/pass
        ("http://github.com/org/repo", "token", "test", "http://proxy.com/org/repo"),
        # already includes username/pass, don't update
        (
            "https://user:pass@github.com/org/repo",
            "token",
            "test",
            "https://user:pass@proxy.com/org/repo",
        ),
    ],
)
def test_add_access_token_and_proxy(repo_url, token, backend, expected, monkeypatch):
    monkeypatch.setattr("common.config.GITHUB_PROXY_DOMAIN", "proxy.com")
    monkeypatch.setattr("agent.config.BACKEND", backend)
    monkeypatch.setattr("common.config.PRIVATE_REPO_ACCESS_TOKEN", token)

    assert add_access_token_and_proxy(repo_url) == expected


@pytest.mark.parametrize(
    "token,exc_cmd,exc_output,exc_stderr,exp_cmd,exp_output,exp_sterr",
    [
        # cmd as list
        (
            "token123",
            ["fetch", "http://user:token123@example.com", "token123"],
            None,
            None,
            ["fetch", "http://user:********@example.com", "********"],
            None,
            None,
        ),
        # no token in config, nothing to redact
        (
            "",
            ["fetch", "http://user:token123@example.com", "token123"],
            None,
            None,
            ["fetch", "http://user:token123@example.com", "token123"],
            None,
            None,
        ),
        # cmd as string, token in output and stderr too
        (
            "token123",
            "cmd with token123",
            "Token token123 in output",
            b"Token token123 in stderr",
            "cmd with ********",
            "Token ******** in output",
            b"Token ******** in stderr",
        ),
        # We can handle a Path command arg
        (
            "token123",
            ["fetch", Path("/test/foo")],
            None,
            None,
            ["fetch", Path("/test/foo")],
            None,
            None,
        ),
    ],
)
def test_redact_token_from_exception(
    token, exc_cmd, exc_output, exc_stderr, exp_cmd, exp_output, exp_sterr, monkeypatch
):
    monkeypatch.setattr("common.config.PRIVATE_REPO_ACCESS_TOKEN", token)
    exception = CalledProcessError(
        1,
        cmd=exc_cmd,
        output=exc_output,
        stderr=exc_stderr,
    )
    redact_token_from_exception(exception)
    assert exception.cmd == exp_cmd
    assert exception.output == exp_output
    assert exception.stderr == exp_sterr


def test_redact_token_from_exception_unhandled_type(monkeypatch):
    monkeypatch.setattr("common.config.PRIVATE_REPO_ACCESS_TOKEN", "token")
    exception = CalledProcessError(
        1,
        cmd=["do", 1],
        output="token",
        stderr=None,
    )
    with pytest.raises(ValueError, match="expected str, bytes or Path"):
        redact_token_from_exception(exception)
