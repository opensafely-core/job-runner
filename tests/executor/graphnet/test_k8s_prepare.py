from pathlib import Path

import pytest

from jobrunner.executors.graphnet.container.prepare import (
    git_clone_and_checkout,
    copy_files
)

REPO_FIXTURE = str(Path(__file__).parents[1].resolve() / "fixtures/git-repo")


@pytest.mark.slow_test
def test_git_clone_and_checkout(tmp_work_dir, tmp_path):
    target_dir = tmp_path / "files"
    
    git_clone_and_checkout(
            "https://github.com/opensafely-core/test-public-repository.git",
            "c1ef0e676ec448b0a49e0073db364f36f6d6d078",
            tmp_work_dir,
            target_dir
    )
    
    assert [f.name for f in target_dir.iterdir()] == ["README.md"]


def test_copy_files(tmp_work_dir, tmp_path):
    src_dir = tmp_work_dir
    
    inputs = [
        "output/foo1",
        "output/foo2",
        "output/foo3",
    ]
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    
    for i in inputs:
        p = (src_dir / i)
        p.parent.mkdir(exist_ok=True, parents=True)
        p.touch()
    
    copy_files(src_dir, inputs, job_dir)
    
    assert set([f.name for f in (job_dir/'output').iterdir()]) == set(Path(f).name for f in inputs)


def test_copy_empty_input_files(tmp_work_dir, tmp_path):
    src_dir = tmp_work_dir
    
    inputs = "".split(";")
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    
    copy_files(src_dir, inputs, job_dir)
    
    assert len([f.name for f in job_dir.iterdir()]) == 0
