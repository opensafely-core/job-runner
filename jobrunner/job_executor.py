from enum import Enum


class Privacy(Enum):
    HIGH = "high"
    MEDIUM = "medium"


def run_job(job_id, image, args, workspace, input_files, env, repo_url, commit, allow_network_access):
    pass


def terminate_job(job_id):
    pass


def get_job_status(job_id, workspace, action, output_spec):
    # return state, status_code, status_message, outputs, unmatched_outputs
    pass


def delete_files(workspace, privacy, filenames):
    pass
