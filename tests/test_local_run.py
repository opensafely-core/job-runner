from pathlib import Path
import shutil

import pytest

from jobrunner import local_run


@pytest.mark.slow_test
@pytest.mark.needs_docker
def test_local_run(tmp_path):
    project_fixture = str(Path(__file__).parent.resolve() / "fixtures/full_project")
    project_dir = tmp_path / "project"
    shutil.copytree(project_fixture, project_dir)
    local_run.main(project_dir=project_dir, actions=["analyse_data"])
    assert (project_dir / "output/input.csv").exists()
    assert (project_dir / "counts.txt").exists()
    assert (project_dir / "metadata/_manifest.json").exists()
    assert (project_dir / "metadata/analyse_data.log").exists()
    assert not (project_dir / "metadata/.logs").exists()
