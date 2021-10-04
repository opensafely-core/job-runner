from __future__ import print_function, unicode_literals, division, absolute_import

import datetime
import glob
import json
import time
from argparse import ArgumentParser
from pathlib import Path

from jobrunner.lib import git
import shutil

from jobrunner.manage_jobs import delete_files, ActionNotRunError, ActionFailedError, MissingOutputError, read_manifest_file, update_manifest
from jobrunner.models import State


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
    
    #     high_privacy_storage_base = work_dir / "high_privacy"
    #     high_privacy_workspace_dir = high_privacy_storage_base / 'workspaces' / workspace_name
    #     high_privacy_metadata_dir = high_privacy_workspace_dir / "metadata"
    #     high_privacy_log_dir = high_privacy_storage_base / 'logs' / datetime.date.today().strftime("%Y-%m") / pod_name
    #     high_privacy_action_log_path = high_privacy_metadata_dir / f"{action}.log"
    #     medium_privacy_storage_base = work_dir / "medium_privacy"
    #     medium_privacy_workspace_dir = medium_privacy_storage_base / 'workspaces' / workspace_name
    #     medium_privacy_metadata_dir = medium_privacy_workspace_dir / "metadata"
    
    parser.add_argument("high_privacy_workspace_dir", type=str, help='workdir/high_privacy/workspaces/{workspace_name}')
    parser.add_argument("high_privacy_metadata_dir", type=str, help='workdir/high_privacy/workspaces/{workspace_name}/metadata')
    parser.add_argument("high_privacy_log_dir", type=str, help='workdir/high_privacy/logs/{datetime.date.today().strftime("%Y-%m")}/{pod_name}')
    parser.add_argument("high_privacy_action_log_path", type=str, help='workdir/high_privacy/workspaces/{workspace_name}/metadata/f"{action}.log"')
    parser.add_argument("medium_privacy_workspace_dir", type=str, help='workdir/medium_privacy/workspaces/{workspace_name}')
    parser.add_argument("medium_privacy_metadata_dir", type=str, help='workdir/medium_privacy/workspaces/{workspace_name}/metadata')
    
    parser.add_argument("execute_logs", type=str, help="log from the execute step")
    parser.add_argument("output_spec_json", type=str, help="JSON of the output_spec")
    parser.add_argument("job_metadata_json", type=str,
                        help="The action log file and any useful metadata from the job run should also be written to a separate log storage area in long-term storage.")
    
    args = parser.parse_args()
    
    job_dir = args.job_dir
    high_privacy_workspace_dir = args.high_privacy_workspace_dir
    high_privacy_metadata_dir = args.high_privacy_metadata_dir
    high_privacy_log_dir = args.high_privacy_log_dir
    high_privacy_action_log_path = args.high_privacy_action_log_path
    medium_privacy_workspace_dir = args.medium_privacy_workspace_dir
    medium_privacy_metadata_dir = args.medium_privacy_metadata_dir
    
    execute_logs = args.execute_logs
    output_spec = json.load(args.output_spec_json)
    job_metadata = json.load(args.job_metadata_json)
    
    job_result = finalize(execute_logs, high_privacy_action_log_path, high_privacy_log_dir, high_privacy_metadata_dir, high_privacy_workspace_dir, job_dir, job_metadata,
                          medium_privacy_metadata_dir, medium_privacy_workspace_dir, output_spec)
    print("__JobResults:__", json.dumps(job_result))


def finalize(execute_logs, high_privacy_action_log_path, high_privacy_log_dir, high_privacy_metadata_dir, high_privacy_workspace_dir, job_dir, job_metadata,
             medium_privacy_metadata_dir, medium_privacy_workspace_dir, output_spec):
    job_dir = Path(job_dir)
    high_privacy_workspace_dir = Path(high_privacy_workspace_dir)
    high_privacy_metadata_dir = Path(high_privacy_metadata_dir)
    high_privacy_log_dir = Path(high_privacy_log_dir)
    high_privacy_action_log_path = Path(high_privacy_action_log_path)
    medium_privacy_workspace_dir = Path(medium_privacy_workspace_dir)
    medium_privacy_metadata_dir = Path(medium_privacy_metadata_dir)
    
    outputs, unmatched_patterns = find_matching_outputs(job_dir, output_spec)
    
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
    
    job_result = {
        'outputs'  : outputs,
        'unmatched': unmatched_patterns,
    }
    return job_result


def find_matching_outputs(job_dir, output_spec):
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


def copy_files(src_dir, src_filenames, dest_dir):
    for src in src_filenames:
        if len(str(src)) > 0:
            if src_dir:
                src_path = Path(src_dir) / src
            else:
                src_path = Path(src)
            
            src.parent.mkdir(exist_ok=True, parents=True)
            
            dest = dest_dir / src.parent
            dest.mkdir(exist_ok=True, parents=True)
            
            shutil.copy(src_path, dest)


if __name__ == '__main__':
    main()
