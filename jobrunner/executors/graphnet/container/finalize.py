from __future__ import print_function, unicode_literals, division, absolute_import

import glob
import json
import shutil
from argparse import ArgumentParser
from pathlib import Path
from typing import Mapping

from jobrunner.executors.graphnet.container.utils import copy_files
from jobrunner.executors.graphnet.k8s import read_log, list_pod_of_job, extract_k8s_api_values, read_k8s_job_status, init_k8s_config, JOB_CONTAINER_NAME

JOB_RESULTS_TAG = "__JobResults__:"


def main():
    """
    Preprocessing before running the opensafely job action
    
    1. save job metadata
    2. Copy logs to workspace
    3. Extract outputs to workspace
    4. Copy out logs and medium privacy files
    """
    
    parser = ArgumentParser(
            description="Preprocess before running the opensafely job action"
    )
    
    parser.add_argument("job_dir", type=str, help="")
    
    parser.add_argument("high_privacy_workspace_dir", type=str, help='workdir/high_privacy/workspaces/{workspace_name}')
    parser.add_argument("high_privacy_metadata_dir", type=str, help='workdir/high_privacy/workspaces/{workspace_name}/metadata')
    parser.add_argument("high_privacy_log_dir", type=str, help='workdir/high_privacy/logs/{datetime.date.today().strftime("%Y-%m")}/{pod_name}')
    parser.add_argument("high_privacy_action_log_path", type=str, help='workdir/high_privacy/workspaces/{workspace_name}/metadata/f"{action}.log"')
    parser.add_argument("medium_privacy_workspace_dir", type=str, help='workdir/medium_privacy/workspaces/{workspace_name}')
    parser.add_argument("medium_privacy_metadata_dir", type=str, help='workdir/medium_privacy/workspaces/{workspace_name}/metadata')
    
    parser.add_argument("output_spec_json", type=str, help="JSON of the output_spec")
    
    parser.add_argument("use_local_k8s_config", type=str, help="")
    parser.add_argument("execute_job_name", type=str, help="")
    parser.add_argument("namespace", type=str, help="")
    parser.add_argument("job_definition_map_json", type=str, help="")
    
    args = parser.parse_args()
    
    job_dir = args.job_dir
    high_privacy_workspace_dir = args.high_privacy_workspace_dir
    high_privacy_metadata_dir = args.high_privacy_metadata_dir
    high_privacy_log_dir = args.high_privacy_log_dir
    high_privacy_action_log_path = args.high_privacy_action_log_path
    medium_privacy_workspace_dir = args.medium_privacy_workspace_dir
    medium_privacy_metadata_dir = args.medium_privacy_metadata_dir
    
    # execute_logs = args.execute_logs
    output_spec = json.loads(args.output_spec_json)
    # job_metadata = json.loads(args.job_metadata_json)
    
    use_local_k8s_config = args.use_local_k8s_config.lower() == 'true'
    execute_job_name = args.execute_job_name
    namespace = args.namespace
    job_definition_map = json.loads(args.job_definition_map_json) if len(args.job_definition_map_json.strip()) > 0 else None
    
    # read the log of the execute job
    batch_v1, core_v1, networking_v1 = init_k8s_config(use_local_k8s_config)
    container_log = None
    logs = read_log(execute_job_name, namespace)
    for (_, container_name), container_log in logs.items():
        if container_name == JOB_CONTAINER_NAME:
            break
    
    # get the metadata of the execute job
    pods = list_pod_of_job(execute_job_name, namespace)
    print(pods)
    job = batch_v1.read_namespaced_job(execute_job_name, namespace)
    job_metadata = extract_k8s_api_values(job, ['env'])  # env contains sql server login
    
    job_status = read_k8s_job_status(execute_job_name, namespace)
    
    execute_logs = container_log
    # output_spec_json = json.dumps(output_spec)
    job_metadata = {
        "state"       : job_status.name,
        "created_at"  : "",
        "started_at"  : str(job.status.start_time),
        "completed_at": str(job.status.completion_time),
        "job_metadata": job_metadata
    }
    if job_definition_map:
        # add fields from JobDefinition
        job_metadata.update(dict(job_definition_map))
    
    # job_metadata_json = json.dumps(job_metadata)
    
    job_result = finalize(job_dir, high_privacy_action_log_path, high_privacy_log_dir, high_privacy_metadata_dir, high_privacy_workspace_dir, medium_privacy_metadata_dir,
                          medium_privacy_workspace_dir, execute_logs, output_spec, job_metadata)
    
    print(JOB_RESULTS_TAG, json.dumps(job_result).replace('\n', ''))


def finalize(
        job_dir,
        high_privacy_action_log_path,
        high_privacy_log_dir,
        high_privacy_metadata_dir,
        high_privacy_workspace_dir,
        medium_privacy_metadata_dir,
        medium_privacy_workspace_dir,
        execute_logs: str,
        output_spec: Mapping[str, str],
        job_metadata: Mapping[str, object]
):
    job_dir = Path(job_dir)
    high_privacy_workspace_dir = Path(high_privacy_workspace_dir)
    high_privacy_metadata_dir = Path(high_privacy_metadata_dir)
    high_privacy_log_dir = Path(high_privacy_log_dir)
    high_privacy_action_log_path = Path(high_privacy_action_log_path)
    medium_privacy_workspace_dir = Path(medium_privacy_workspace_dir)
    medium_privacy_metadata_dir = Path(medium_privacy_metadata_dir)
    
    outputs, unmatched_patterns = find_matching_outputs(job_dir, output_spec)
    job_result = {
        'outputs'  : {str(filename): privacy_level for filename, privacy_level in outputs.items()},
        'unmatched': unmatched_patterns,
    }
    
    job_metadata = dict(job_metadata)
    job_metadata['outputs'] = job_result['outputs']
    job_metadata['status_message'] = job_result['unmatched']
    
    # high privacy
    high_privacy_log_dir.mkdir(parents=True, exist_ok=True)
    high_privacy_workspace_dir.mkdir(parents=True, exist_ok=True)
    high_privacy_metadata_dir.mkdir(parents=True, exist_ok=True)
    high_privacy_action_log_path.parent.mkdir(parents=True, exist_ok=True)
    medium_privacy_metadata_dir.mkdir(parents=True, exist_ok=True)
    medium_privacy_workspace_dir.mkdir(parents=True, exist_ok=True)
    
    with open(high_privacy_log_dir / "logs.txt", 'w+') as f:
        f.write(execute_logs)
    
    with open(high_privacy_log_dir / "metadata.json", "w+") as f:
        json.dump(job_metadata, f, indent=2)
    
    with open(high_privacy_action_log_path, 'w+') as f:
        f.write(execute_logs)
    
    copy_files(job_dir, outputs.keys(), high_privacy_workspace_dir)
    
    # medium privacy
    shutil.copy(high_privacy_action_log_path, medium_privacy_metadata_dir)
    
    medium_privacy_files = [filename for filename, privacy_level in outputs.items() if privacy_level == "moderately_sensitive"]
    copy_files(high_privacy_workspace_dir, medium_privacy_files, medium_privacy_workspace_dir)
    
    return job_result


def find_matching_outputs_old(job_dir: str, output_spec: Mapping[str, Mapping[str, str]]):
    """
    Returns a dict mapping output filenames to their privacy level, plus a list
    of any patterns that had no matches at all
    """
    all_patterns = []
    for privacy_level, named_patterns in output_spec.items():
        for name, pattern in named_patterns.items():
            all_patterns.append(pattern)
    
    all_matches = {pattern: [Path(full_path).relative_to(job_dir) for full_path in glob.glob(f"{job_dir}/{pattern}")] for pattern in all_patterns}
    
    unmatched_patterns = []
    outputs = {}
    for privacy_level, named_patterns in output_spec.items():
        for name, pattern in named_patterns.items():
            filenames = all_matches[pattern]
            if not filenames:
                unmatched_patterns.append(pattern)
            for filename in filenames:
                outputs[filename] = privacy_level
    return outputs, unmatched_patterns


def find_matching_outputs(job_dir: Path, output_spec: Mapping[str, str]):
    """
    Returns a dict mapping output filenames to their privacy level, plus a list
    of any patterns that had no matches at all
    """
    all_patterns = []
    for pattern, privacy_level in output_spec.items():
        all_patterns.append(pattern)
    
    all_matches = {pattern: [Path(full_path).relative_to(job_dir) for full_path in glob.glob(f"{job_dir}/{pattern}")] for pattern in all_patterns}
    
    unmatched_patterns = []
    outputs = {}
    for pattern, privacy_level in output_spec.items():
        filenames = all_matches[pattern]
        if not filenames:
            unmatched_patterns.append(pattern)
        for filename in filenames:
            outputs[filename] = privacy_level
    return outputs, unmatched_patterns


if __name__ == '__main__':
    main()
