import datetime
from pathlib import Path

from jobrunner.executors.graphnet.container.finalize import (
    finalize
)


def test_finalize(tmp_path):
    tmp_dir = Path(tmp_path)
    
    job_dir = tmp_dir / 'jobdir'
    work_dir = tmp_dir / 'workdir'
    workspace_name = 'sro-measure-int-test'
    action = 'generate_study_population'
    pod_name = 'job-opensafely-sro-measures-generate-study-population-52tngak35bwsso2v'
    
    high_privacy_storage_base = work_dir / "high_privacy"
    medium_privacy_storage_base = work_dir / "medium_privacy"
    
    high_privacy_workspace_dir = high_privacy_storage_base / 'workspaces' / workspace_name
    high_privacy_metadata_dir = high_privacy_workspace_dir / "metadata"
    high_privacy_log_dir = high_privacy_storage_base / 'logs' / datetime.date.today().strftime("%Y-%m") / pod_name
    high_privacy_action_log_path = high_privacy_metadata_dir / f"{action}.log"
    medium_privacy_workspace_dir = medium_privacy_storage_base / 'workspaces' / workspace_name
    medium_privacy_metadata_dir = medium_privacy_workspace_dir / "metadata"
    
    execute_logs = "some logs"
    # output_spec = {'highly_sensitive': {'cohort': 'output/input_*.csv'}, 'moderately_sensitive': {'cohort': 'test/medium_privacy_file.txt'}}
    output_spec = {'output/input_*.csv': 'highly_sensitive', 'test/medium_privacy_file.txt': 'moderately_sensitive'}
    job_metadata = {'useful': 'finformation'}
    
    # generate test csv files
    test_output_file = ['output/input_2021-01-01.csv', 'output/input_2021-02-01.csv', 'test/medium_privacy_file.txt']
    test_unmatch_file = ['unmatch/file1.txt', 'output/input.csv']
    for n in test_output_file:
        target = job_dir / n
        target.parent.mkdir(exist_ok=True, parents=True)
        with open(target, 'w+') as f:
            f.write(n)
    for n in test_unmatch_file:
        target = job_dir / n
        target.parent.mkdir(exist_ok=True, parents=True)
        with open(target, 'w+') as f:
            f.write(n)
    
    job_result = finalize(job_dir, high_privacy_action_log_path, high_privacy_log_dir, high_privacy_metadata_dir, high_privacy_workspace_dir, medium_privacy_metadata_dir,
                          medium_privacy_workspace_dir, execute_logs, output_spec, job_metadata)
    
    print(job_result)
    
    assert (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/output/input_2021-01-01.csv').exists()
    assert (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/output/input_2021-02-01.csv').exists()
    assert (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/metadata/{action}.log').exists()
    assert (tmp_path / f'workdir/high_privacy/logs/{datetime.date.today().strftime("%Y-%m")}/{pod_name}/logs.txt').exists()
    assert (tmp_path / f'workdir/high_privacy/logs/{datetime.date.today().strftime("%Y-%m")}/{pod_name}/metadata.json').exists()
    assert not (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/unmatch/files1.txt').exists()
    assert not (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/output/input.txt').exists()
    
    assert (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/metadata/{action}.log').exists()
    assert (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/test/medium_privacy_file.txt').exists()
    assert not (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/output/input_2021-01-01.csv').exists()
    assert not (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/output/input_2021-02-01.csv').exists()
    assert not (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/unmatch/files1.txt').exists()
    assert not (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/output/input.txt').exists()


def test_finalize_empty_output(tmp_path):
    tmp_dir = Path(tmp_path)
    
    job_dir = tmp_dir / 'jobdir'
    work_dir = tmp_dir / 'workdir'
    workspace_name = 'sro-measure-int-test'
    action = 'generate_study_population'
    pod_name = 'job-opensafely-sro-measures-generate-study-population-52tngak35bwsso2v'
    
    high_privacy_storage_base = work_dir / "high_privacy"
    medium_privacy_storage_base = work_dir / "medium_privacy"
    
    high_privacy_workspace_dir = high_privacy_storage_base / 'workspaces' / workspace_name
    high_privacy_metadata_dir = high_privacy_workspace_dir / "metadata"
    high_privacy_log_dir = high_privacy_storage_base / 'logs' / datetime.date.today().strftime("%Y-%m") / pod_name
    high_privacy_action_log_path = high_privacy_metadata_dir / f"{action}.log"
    medium_privacy_workspace_dir = medium_privacy_storage_base / 'workspaces' / workspace_name
    medium_privacy_metadata_dir = medium_privacy_workspace_dir / "metadata"
    
    execute_logs = "some logs"
    output_spec = {}
    job_metadata = {'useful': 'finformation'}
    
    # generate test csv files
    test_output_file = ['output/input_2021-01-01.csv', 'output/input_2021-02-01.csv', 'test/medium_privacy_file.txt']
    test_unmatch_file = ['unmatch/file1.txt', 'output/input.csv']
    for n in test_output_file:
        target = job_dir / n
        target.parent.mkdir(exist_ok=True, parents=True)
        with open(target, 'w+') as f:
            f.write(n)
    for n in test_unmatch_file:
        target = job_dir / n
        target.parent.mkdir(exist_ok=True, parents=True)
        with open(target, 'w+') as f:
            f.write(n)
    
    job_result = finalize(job_dir, high_privacy_action_log_path, high_privacy_log_dir, high_privacy_metadata_dir, high_privacy_workspace_dir, medium_privacy_metadata_dir,
                          medium_privacy_workspace_dir, execute_logs, output_spec, job_metadata)
    
    print(job_result)
    
    assert not (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/output/input_2021-01-01.csv').exists()
    assert not (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/output/input_2021-02-01.csv').exists()
    assert (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/metadata/{action}.log').exists()
    assert (tmp_path / f'workdir/high_privacy/logs/{datetime.date.today().strftime("%Y-%m")}/{pod_name}/logs.txt').exists()
    assert (tmp_path / f'workdir/high_privacy/logs/{datetime.date.today().strftime("%Y-%m")}/{pod_name}/metadata.json').exists()
    assert not (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/unmatch/files1.txt').exists()
    assert not (tmp_path / f'workdir/high_privacy/workspaces/sro-measure-int-test/output/input.txt').exists()
    
    assert (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/metadata/{action}.log').exists()
    assert not (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/test/medium_privacy_file.txt').exists()
    assert not (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/output/input_2021-01-01.csv').exists()
    assert not (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/output/input_2021-02-01.csv').exists()
    assert not (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/unmatch/files1.txt').exists()
    assert not (tmp_path / f'workdir/medium_privacy/workspaces/sro-measure-int-test/output/input.txt').exists()
