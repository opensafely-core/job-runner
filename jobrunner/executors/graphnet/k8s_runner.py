from __future__ import print_function, unicode_literals, division, absolute_import

import dataclasses
import datetime
import json
import logging
import re
import time
from pathlib import Path

from kubernetes import client

from jobrunner import config
from jobrunner.executors.graphnet import config as graphnet_config
from jobrunner.executors.graphnet import k8s
from jobrunner.executors.graphnet.k8s import (
    init_k8s_config,
    convert_k8s_name,
    create_namespace,
    create_pv,
    create_pvc,
    create_k8s_job,
    is_pvc_created,
    create_network_policy,
    read_k8s_job_status,
    K8SJobStatus,
    read_log,
    read_finalize_output,
    read_container_exit_code,
    JOB_CONTAINER_NAME,
    read_image_id,
    await_job_status,
    list_pod_of_job,
    extract_k8s_api_values,
)
from jobrunner.job_executor import *

log = logging.getLogger(__name__)

WORK_DIR = "/workdir"
JOB_DIR = "/workspace"

batch_v1: client.BatchV1Api
core_v1: client.CoreV1Api
networking_v1: client.NetworkingV1Api


class K8SJobAPI(ExecutorAPI):
    def __init__(self):
        global batch_v1, core_v1, networking_v1
        batch_v1, core_v1, networking_v1 = init_k8s_config(graphnet_config.GRAPHNET_K8S_USE_LOCAL_CONFIG)
    
    def prepare(self, job: JobDefinition) -> JobStatus:
        try:
            # 1. Validate the JobDefinition. If there are errors, return an ERROR state with message.
            key_entries = [job.workspace, job.id, job.action]
            for e in key_entries:
                if e is None or len(e.strip()) == 0:
                    raise Exception(f"empty values found in key_entries [job.workspace, job.id, job.action]={[job.workspace, job.id, job.action]}")
            
            prepare_job_name = get_prepare_job_name(job)
            
            # 2. Check the job is currently in UNKNOWN state. If not return its current state with a message indicated invalid state.
            status = self.get_status(job)
            
            if status.state in [ExecutorState.PREPARING, ExecutorState.PREPARED]:
                return JobStatus(status.state, f"already in state {status.state}")
            if status.state != ExecutorState.UNKNOWN:
                return JobStatus(status.state, f"invalid operation finalize() in state {status.state}")
            
            work_pv = get_work_pv_name(job)
            job_pv = get_job_pv_name(job)
            
            # 3. Check the resources are available to prepare the job. If not, return the UNKNOWN state with an appropriate message.
            storage_class = graphnet_config.GRAPHNET_K8S_STORAGE_CLASS
            ws_pv_size = graphnet_config.GRAPHNET_K8S_WS_STORAGE_SIZE
            job_pv_size = graphnet_config.GRAPHNET_K8S_JOB_STORAGE_SIZE
            host_path = {"path": f"/tmp/{str(int(time.time() * 10 ** 6))}"} if graphnet_config.GRAPHNET_K8S_USE_LOCAL_CONFIG else None
            create_pv(work_pv, storage_class, ws_pv_size, host_path)
            create_pv(job_pv, storage_class, job_pv_size, host_path)
            
            namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
            create_namespace(namespace)
            
            work_pvc = get_work_pvc_name(job)
            job_pvc = get_job_pvc_name(job)
            
            # 4. Create an ephemeral workspace to use for executing this job. This is expected to be a volume mounted into the container,
            # but other implementations are allowed.
            create_pvc(work_pv, work_pvc, storage_class, namespace, ws_pv_size)
            create_pvc(job_pv, job_pvc, storage_class, namespace, job_pv_size)
            
            commit_sha = job.study.commit
            inputs = ";".join(job.inputs)
            repo_url = job.study.git_repo_url
            
            # 5. Launch a prepare task asynchronously. If launched successfully, return the PREPARING state. If not, return an ERROR state with message.
            private_repo_access_token = config.PRIVATE_REPO_ACCESS_TOKEN
            prepare(prepare_job_name, commit_sha, inputs, job_pvc, private_repo_access_token, repo_url, work_pvc)
            
            return JobStatus(ExecutorState.PREPARING)
        except Exception as e:
            if config.DEBUG == 1:
                raise e
            else:
                log.exception(str(e))
                return JobStatus(ExecutorState.ERROR, str(e))
    
    def execute(self, job: JobDefinition) -> JobStatus:
        try:
            # 1. Check the job is in the PREPARED state. If not, return its current state with a message.
            status = self.get_status(job)
            
            if status.state in [ExecutorState.EXECUTING, ExecutorState.EXECUTED]:
                return JobStatus(status.state, f"already in state {status.state}")
            if status.state != ExecutorState.PREPARED:
                return JobStatus(status.state, f"invalid operation finalize() in state {status.state}")
            
            # 2. Validate that the ephememeral workspace created by prepare for this job exists.  If not, return an ERROR state with message.
            job_pvc = get_job_pvc_name(job)
            if not is_pvc_created(job_pvc):
                return JobStatus(ExecutorState.ERROR, f"PVC not found {job_pvc}")
            
            # 3. Check there are resources availabe to execute the job. If not, return PREPARED status with an appropriate message.
            execute_job_name = get_execute_job_name(job)
            execute_job_arg = [job.action] + job.args
            execute_job_command = None
            execute_job_env = dict(job.env)
            execute_job_image = job.image
            
            # add DB URL for cohortextractor
            if job.allow_database_access:
                execute_job_env['DATABASE_URL'] = config.DATABASE_URLS['full']
            
            namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
            whitelist = graphnet_config.GRAPHNET_K8S_EXECUTION_HOST_WHITELIST
            if job.allow_database_access and len(whitelist.strip()) > 0:
                network_labels = create_network_policy(namespace, [ip_port.split(":") for ip_port in whitelist.split(",")])  # allow whitelist
            else:
                network_labels = create_network_policy(namespace, [])  # deny all
            
            # 4. Launch the job execution task asynchronously. If launched successfully, return the EXECUTING state. If not, return an ERROR state with message.
            execute(execute_job_name, execute_job_arg, execute_job_command, execute_job_env, execute_job_image, job_pvc, network_labels)
            
            return JobStatus(ExecutorState.EXECUTING)
        except Exception as e:
            if config.DEBUG == 1:
                raise e
            else:
                log.exception(str(e))
                return JobStatus(ExecutorState.ERROR, str(e))
    
    def finalize(self, job: JobDefinition) -> JobStatus:
        try:
            # 1. Check the job is in the EXECUTED state. If not, return its current state with a message.
            status = self.get_status(job)
            
            if status.state in [ExecutorState.FINALIZING, ExecutorState.FINALIZED]:
                return JobStatus(status.state, f"already in state {status.state}")
            if status.state != ExecutorState.EXECUTED:
                return JobStatus(status.state, f"invalid operation finalize() in state {status.state}")
            
            # 2. Validate that the job's ephemeral workspace exists. If not, return an ERROR state with message.
            job_pvc = get_job_pvc_name(job)
            if not is_pvc_created(job_pvc):
                return JobStatus(ExecutorState.ERROR, f"PVC not found {job_pvc}")
            
            # 3. Launch the finalize task asynchronously. If launched successfully, return the FINALIZING state. If not, return an ERROR state with message.
            action = job.action
            execute_job_name = get_execute_job_name(job)
            job_pvc = get_job_pvc_name(job)
            work_pvc = get_work_pvc_name(job)
            opensafely_job_id = job.id
            opensafely_job_name = get_opensafely_job_name(job)
            output_spec = job.output_spec
            workspace_name = job.workspace
            
            finalize_job_name = convert_k8s_name(opensafely_job_name, "finalize", additional_hash=opensafely_job_id)
            finalize(finalize_job_name, action, execute_job_name, job_pvc, output_spec, work_pvc, workspace_name, job)
            return JobStatus(ExecutorState.FINALIZING)
        except Exception as e:
            if config.DEBUG == 1:
                raise e
            else:
                log.exception(str(e))
                return JobStatus(ExecutorState.ERROR, str(e))
    
    def get_status(self, job: JobDefinition) -> JobStatus:
        namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
        
        prepare_job_name = get_prepare_job_name(job)
        prepare_state = read_k8s_job_status(prepare_job_name, namespace)
        if prepare_state == K8SJobStatus.UNKNOWN:
            return JobStatus(ExecutorState.UNKNOWN)
        elif prepare_state == K8SJobStatus.PENDING or prepare_state == K8SJobStatus.RUNNING:
            return JobStatus(ExecutorState.PREPARING)
        elif prepare_state == K8SJobStatus.FAILED:
            try:
                logs = read_log(prepare_job_name, namespace)
                return JobStatus(ExecutorState.ERROR, json.dumps(logs))
            except Exception as e:
                return JobStatus(ExecutorState.ERROR, str(e))
        elif prepare_state == K8SJobStatus.SUCCEEDED:
            
            execute_job_name = get_execute_job_name(job)
            execute_state = read_k8s_job_status(execute_job_name, namespace)
            if execute_state == K8SJobStatus.UNKNOWN:
                return JobStatus(ExecutorState.PREPARED)
            elif execute_state == K8SJobStatus.PENDING or execute_state == K8SJobStatus.RUNNING:
                return JobStatus(ExecutorState.EXECUTING)
            elif execute_state == K8SJobStatus.FAILED:
                try:
                    logs = read_log(execute_job_name, namespace)
                    return JobStatus(ExecutorState.ERROR, json.dumps(logs))
                except Exception as e:
                    return JobStatus(ExecutorState.ERROR, str(e))
            elif execute_state == K8SJobStatus.SUCCEEDED:
                
                finalize_job_name = get_finalize_job_name(job)
                finalize_state = read_k8s_job_status(finalize_job_name, namespace)
                if finalize_state == K8SJobStatus.UNKNOWN:
                    return JobStatus(ExecutorState.EXECUTED)
                elif finalize_state == K8SJobStatus.PENDING or finalize_state == K8SJobStatus.RUNNING:
                    return JobStatus(ExecutorState.FINALIZING)
                elif finalize_state == K8SJobStatus.FAILED:
                    try:
                        logs = read_log(finalize_job_name, namespace)
                        return JobStatus(ExecutorState.ERROR, json.dumps(logs))
                    except Exception as e:
                        return JobStatus(ExecutorState.ERROR, str(e))
                elif finalize_state == K8SJobStatus.SUCCEEDED:
                    return JobStatus(ExecutorState.FINALIZED)
        
        # should not happen
        return JobStatus(ExecutorState.ERROR, "Unknown status found in get_status()")
    
    def terminate(self, job: JobDefinition) -> JobStatus:
        # 1. If any task for this job is running, terminate it, do not wait for it to complete.
        jobs_deleted = delete_all_jobs(job)
        
        # 2. Return ERROR state with a message.
        return JobStatus(ExecutorState.ERROR, f"deleted {','.join(jobs_deleted)}")
    
    def cleanup(self, job: JobDefinition) -> JobStatus:
        # 1. Initiate the cleanup, do not wait for it to complete.
        namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
        
        delete_all_jobs(job)
        
        job_pvc = get_job_pvc_name(job)
        job_pv = get_job_pv_name(job)
        
        try:
            core_v1.delete_namespaced_persistent_volume_claim(job_pvc, namespace)
        except:  # already deleted
            pass
        
        try:
            core_v1.delete_persistent_volume(job_pv)
        except:  # already deleted
            pass
        
        # 2. Return the UNKNOWN status.
        return JobStatus(ExecutorState.UNKNOWN)
    
    def get_results(self, job: JobDefinition) -> JobResults:
        namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
        
        job_output = read_finalize_output(get_opensafely_job_name(job), job.id, namespace)
        # finalize_status = read_k8s_job_status(self.get_finalize_job_name(job), namespace)
        
        # extract image id
        job_name = get_execute_job_name(job)
        container_name = JOB_CONTAINER_NAME
        
        exit_code = read_container_exit_code(job_name, container_name, namespace)
        
        image_id = read_image_id(job_name, container_name, namespace)
        if image_id:
            result = re.search(r'@(.+:.+)', image_id)
            if result:
                image_id = result.group(1)
        
        return JobResults(
                job_output['outputs'],
                job_output['unmatched'],
                exit_code,
                image_id
        )
    
    def delete_files(self, workspace: str, privacy: Privacy, paths: [str]):
        try:
            namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
            work_pvc = convert_k8s_name(workspace, "pvc")
            job_name = delete_work_files(workspace, privacy, paths, work_pvc, namespace)
            status = await_job_status(job_name, namespace)
            if status != K8SJobStatus.SUCCEEDED:
                raise Exception(f"unable to delete_files {workspace} {privacy} {paths} {job_name}")
            # have to delete it here as this job will not be able to identified in the clean up
            k8s.delete_job(job_name, namespace)
        except Exception as e:
            log.exception(e)


def prepare(prepare_job_name, commit_sha, inputs, job_pvc, private_repo_access_token, repo_url, work_pvc):
    repos_dir = WORK_DIR + "/repos"
    command = ['python', '-m', 'jobrunner.prepare']
    args = [repo_url, commit_sha, repos_dir, JOB_DIR, inputs]
    env = {'PRIVATE_REPO_ACCESS_TOKEN': private_repo_access_token}
    storages = [
        (work_pvc, WORK_DIR, False),
        (job_pvc, JOB_DIR, True),
    ]
    image_pull_policy = "Never" if graphnet_config.GRAPHNET_K8S_USE_LOCAL_CONFIG else "IfNotPresent"
    namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
    jobrunner_image = graphnet_config.GRAPHNET_K8S_JOB_RUNNER_IMAGE
    create_k8s_job(prepare_job_name, namespace, jobrunner_image, command, args, env, storages, {}, image_pull_policy=image_pull_policy)
    return prepare_job_name


def execute(execute_job_name, execute_job_arg, execute_job_command, execute_job_env, execute_job_image, job_pvc, network_labels):
    command = execute_job_command
    args = execute_job_arg
    storages = [
        (job_pvc, JOB_DIR, True),
    ]
    env = execute_job_env
    image_pull_policy = "Never" if graphnet_config.GRAPHNET_K8S_USE_LOCAL_CONFIG else "IfNotPresent"
    namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
    create_k8s_job(execute_job_name, namespace, execute_job_image, command, args, env, storages, network_labels, image_pull_policy=image_pull_policy)
    return execute_job_name


def finalize(finalize_job_name, action, execute_job_name, job_pvc, output_spec, work_pvc, workspace_name, job_definition: JobDefinition = None):
    # read the log of the execute job
    pod_name = None
    container_log = None
    namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
    logs = read_log(execute_job_name, namespace)
    for (pod_name, container_name), container_log in logs.items():
        if container_name == JOB_CONTAINER_NAME:
            break
    
    # get the metadata of the execute job
    pods = list_pod_of_job(execute_job_name, namespace)
    print(pods)
    
    job = batch_v1.read_namespaced_job(execute_job_name, namespace)
    job_metadata = extract_k8s_api_values(job, ['env'])  # env contains sql server login
    
    job_status = read_k8s_job_status(execute_job_name, namespace)
    
    high_privacy_storage_base = Path(WORK_DIR) / "high_privacy"
    high_privacy_workspace_dir = high_privacy_storage_base / 'workspaces' / workspace_name
    high_privacy_metadata_dir = high_privacy_workspace_dir / "metadata"
    high_privacy_log_dir = high_privacy_storage_base / 'logs' / datetime.date.today().strftime("%Y-%m") / pod_name
    high_privacy_action_log_path = high_privacy_metadata_dir / f"{action}.log"
    
    medium_privacy_storage_base = Path(WORK_DIR) / "medium_privacy"
    medium_privacy_workspace_dir = medium_privacy_storage_base / 'workspaces' / workspace_name
    medium_privacy_metadata_dir = medium_privacy_workspace_dir / "metadata"
    
    execute_logs = container_log
    output_spec_json = json.dumps(output_spec)
    job_metadata = {
        "state"       : job_status.name,
        "created_at"  : "",
        "started_at"  : str(job.status.start_time),
        "completed_at": str(job.status.completion_time),
        "job_metadata": job_metadata
    }
    if job_definition:
        # add fields from JobDefinition
        job_metadata.update(dataclasses.asdict(job_definition))
    
    job_metadata_json = json.dumps(job_metadata)
    
    command = ['python', '-m', 'jobrunner.finalize']
    args = [JOB_DIR, high_privacy_workspace_dir, high_privacy_metadata_dir, high_privacy_log_dir, high_privacy_action_log_path, medium_privacy_workspace_dir,
            medium_privacy_metadata_dir, execute_logs, output_spec_json, job_metadata_json]
    args = [str(a) for a in args]
    env = {}
    storages = [
        (work_pvc, WORK_DIR, False),
        (job_pvc, JOB_DIR, True),
    ]
    image_pull_policy = "Never" if graphnet_config.GRAPHNET_K8S_USE_LOCAL_CONFIG else "IfNotPresent"
    jobrunner_image = graphnet_config.GRAPHNET_K8S_JOB_RUNNER_IMAGE
    create_k8s_job(finalize_job_name, namespace, jobrunner_image, command, args, env, storages, {}, image_pull_policy=image_pull_policy)
    
    return finalize_job_name


def delete_work_files(workspace, privacy, paths, work_pvc, namespace):
    job_name = convert_k8s_name(workspace, f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}-delete-job", additional_hash=";".join(paths))
    if privacy == Privacy.HIGH:
        workspace_dir = Path(WORK_DIR) / "high_privacy" / 'workspaces' / workspace
    else:
        workspace_dir = Path(WORK_DIR) / "medium_privacy" / 'workspaces' / workspace
    image = "busybox"
    command = ['/bin/sh', '-c']
    args = [';'.join([f'rm -f {workspace_dir / p} || true' for p in paths])]
    storage = [
        # pvc, path, is_control
        (work_pvc, WORK_DIR, False)
    ]
    image_pull_policy = "Never" if graphnet_config.GRAPHNET_K8S_USE_LOCAL_CONFIG else "IfNotPresent"
    create_k8s_job(job_name, namespace, image, command, args, {}, storage, dict(), image_pull_policy=image_pull_policy)
    return job_name


def delete_all_jobs(job):
    namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
    
    jobs_deleted = []
    prepare_job_name = get_prepare_job_name(job)
    try:
        batch_v1.delete_namespaced_job(prepare_job_name, namespace)
        jobs_deleted.append(prepare_job_name)
    except:  # already deleted
        pass
    execute_job_name = get_execute_job_name(job)
    try:
        batch_v1.delete_namespaced_job(execute_job_name, namespace)
        jobs_deleted.append(execute_job_name)
    except:  # already deleted
        pass
    finalize_job_name = get_finalize_job_name(job)
    try:
        batch_v1.delete_namespaced_job(finalize_job_name, namespace)
        jobs_deleted.append(finalize_job_name)
    except:  # already deleted
        pass
    return jobs_deleted


def get_work_pv_name(job):
    return convert_k8s_name(job.workspace, "pv")


def get_job_pv_name(job):
    return convert_k8s_name(job.id, "pv")


def get_job_pvc_name(job):
    return convert_k8s_name(job.id, "pvc")


def get_work_pvc_name(job):
    return convert_k8s_name(job.workspace, "pvc")


def get_opensafely_job_name(job):
    return f"{job.workspace}_{job.action}"


def get_execute_job_name(job):
    return convert_k8s_name(get_opensafely_job_name(job), "execute", additional_hash=job.id)


def get_prepare_job_name(job):
    return convert_k8s_name(get_opensafely_job_name(job), "prepare", additional_hash=job.id)


def get_finalize_job_name(job):
    return convert_k8s_name(get_opensafely_job_name(job), "finalize", additional_hash=job.id)
