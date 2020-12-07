from pathlib import Path
import os
import shutil
import sys

import pytest

from jobrunner import local_run
from jobrunner.subprocess_utils import subprocess_run


@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_local_run(tmp_path):
    project_fixture = str(Path(__file__).parent.resolve() / "fixtures/full_project")
    project_dir = tmp_path / "project"
    shutil.copytree(project_fixture, project_dir)
    local_run.main(project_dir=project_dir, actions=["analyse_data"])
    assert (project_dir / "output/input.csv").exists()
    assert (project_dir / "counts.txt").exists()
    assert (project_dir / "metadata/manifest.json").exists()
    assert (project_dir / "metadata/analyse_data.log").exists()
    assert not (project_dir / "metadata/.logs").exists()


@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_local_run_stata(tmp_path, monkeypatch):
    project_fixture = str(Path(__file__).parent.resolve() / "fixtures/stata_project")
    project_dir = tmp_path / "project"
    shutil.copytree(project_fixture, project_dir)
    monkeypatch.setattr("jobrunner.config.STATA_LICENSE", 'env-license')
    assert local_run.main(project_dir=project_dir, actions=["stata"])
    env_file = (project_dir / "output/env.txt")
    assert env_file.read_text() == 'env-license'


@pytest.fixture
def systmpdir(monkeypatch, tmp_path):
    """Set the system tempdir to tmp_path for this test, for isolation."""
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))


def test_get_stata_license_cache_exists(systmpdir, monkeypatch, tmp_path):

    def fail(*a, **kwargs):
        assert False, "should not have been called"

    monkeypatch.setattr("jobrunner.subprocess_utils.subprocess_run", fail)
    cache = tmp_path / "opensafely-stata.lic"
    cache.write_text("cached-license")
    assert local_run.get_stata_license() == "cached-license"


def test_get_stata_license_repo_fetch(systmpdir, tmp_path):
    # create a repo to clone the license from
    repo = tmp_path / "test-repo"
    repo.mkdir()
    license = repo / "stata.lic"
    license.write_text("repo-license")
    git = ['git', '-c', 'user.name=test', '-c', 'user.email=test@example.com']
    cwd = str(repo)
    subprocess_run(git + ["init"], cwd=cwd) 
    subprocess_run(git + ["add", "stata.lic"], cwd=cwd)
    subprocess_run(git + ["commit", "-m", "test"], cwd=cwd)
    assert local_run.get_stata_license(cwd) == "repo-license"
    assert (tmp_path / 'opensafely-stata.lic').read_text() == "repo-license"


def test_get_stata_license_repo_error(systmpdir):
    # GH auth errors are exposed as not found errors, so this is close-ish to a
    # real git auth failure condition.
    with pytest.raises(Exception) as e:
        local_run.get_stata_license('/invalid/repo')
