import argparse
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from jobrunner.cli import local_run
from jobrunner.lib.subprocess_utils import subprocess_run

FIXTURE_DIR = Path(__file__).parents[1].resolve() / "fixtures"


@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_local_run(tmp_path):
    project_dir = tmp_path / "project"
    shutil.copytree(str(FIXTURE_DIR / "full_project"), project_dir)
    local_run.main(project_dir=project_dir, actions=["analyse_data"])
    assert (project_dir / "output/input.csv").exists()
    assert (project_dir / "counts.txt").exists()
    assert (project_dir / "metadata/analyse_data.log").exists()
    assert not (project_dir / "metadata/.logs").exists()


@pytest.mark.slow_test
@pytest.mark.needs_docker
@pytest.mark.skipif(
    not os.environ.get("STATA_LICENSE"), reason="No STATA_LICENSE env var"
)
def test_local_run_stata(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    shutil.copytree(str(FIXTURE_DIR / "stata_project"), project_dir)
    monkeypatch.setattr("jobrunner.config.STATA_LICENSE", os.environ["STATA_LICENSE"])
    local_run.main(project_dir=project_dir, actions=["stata"])
    env_file = project_dir / "output/env.txt"
    assert "Bennett Institute" in env_file.read_text()


@pytest.fixture
def systmpdir(monkeypatch, tmp_path):
    """Set the system tempdir to tmp_path for this test, for isolation."""
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))


@pytest.fixture
def license_repo(tmp_path):
    # create a repo to clone the license from
    repo = tmp_path / "test-repo"
    repo.mkdir()
    license = repo / "stata.lic"
    license.write_text("repo-license")
    git = ["git", "-c", "user.name=test", "-c", "user.email=test@example.com"]
    repo_path = str(repo)
    subprocess_run(git + ["init"], cwd=repo_path)
    subprocess_run(git + ["add", "stata.lic"], cwd=repo_path)
    subprocess_run(git + ["commit", "-m", "test"], cwd=repo_path)
    return repo_path


def test_get_stata_license_cache_recent(systmpdir, monkeypatch, tmp_path):
    def fail(*a, **kwargs):
        assert False, "should not have been called"

    monkeypatch.setattr("jobrunner.lib.subprocess_utils.subprocess_run", fail)
    cache = tmp_path / "opensafely-stata.lic"
    cache.write_text("cached-license")
    assert local_run.get_stata_license() == "cached-license"


def test_get_stata_license_cache_expired(systmpdir, tmp_path, license_repo):
    cache = tmp_path / "opensafely-stata.lic"
    cache.write_text("cached-license")
    utime = (datetime.utcnow() - timedelta(hours=12)).timestamp()
    os.utime(cache, (utime, utime))

    assert local_run.get_stata_license(license_repo) == "repo-license"
    assert (tmp_path / "opensafely-stata.lic").read_text() == "repo-license"


def test_get_stata_license_repo_fetch(systmpdir, tmp_path, license_repo):
    assert local_run.get_stata_license(license_repo) == "repo-license"
    assert (tmp_path / "opensafely-stata.lic").read_text() == "repo-license"


def test_get_stata_license_repo_error(systmpdir):
    assert local_run.get_stata_license("/invalid/repo") is None


def test_add_arguments():
    parser = argparse.ArgumentParser(description="test")
    parser = local_run.add_arguments(parser)
    args = parser.parse_args(
        [
            "action",
            "--force-run-dependencies",
            "--project-dir=dir",
            "--continue-on-error",
            "--timestamps",
            "--debug",
        ]
    )

    assert args.actions == ["action"]
    assert args.force_run_dependencies
    assert args.project_dir == "dir"
    assert args.timestamps
    assert args.debug
