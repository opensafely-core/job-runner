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
from jobrunner.executors.graphnet.container.finalize import JOB_RESULTS_TAG
from jobrunner.executors.graphnet import k8s
from jobrunner.executors.graphnet.k8s import K8SJobStatus
from jobrunner.job_executor import *

log = logging.getLogger(__name__)

WORK_DIR = "/workdir"
JOB_DIR = "/workspace"

OPENSAFELY_LABEL_TAG = "opensafely-app"

batch_v1: client.BatchV1Api
core_v1: client.CoreV1Api
networking_v1: client.NetworkingV1Api


class K8SExecutorAPI(ExecutorAPI):
    def __init__(self):
        global batch_v1, core_v1, networking_v1
        batch_v1, core_v1, networking_v1 = k8s.init_k8s_config(graphnet_config.GRAPHNET_K8S_USE_LOCAL_CONFIG)
    
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
            
            namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
            k8s.create_namespace(namespace)
            
            work_pvc = get_work_pvc_name(job)
            job_pvc = get_job_pvc_name(job)
            
            # 3. Check the resources are available to prepare the job. If not, return the UNKNOWN state with an appropriate message.
            storage_class = graphnet_config.GRAPHNET_K8S_STORAGE_CLASS
            ws_pv_size = graphnet_config.GRAPHNET_K8S_WS_STORAGE_SIZE
            job_pv_size = graphnet_config.GRAPHNET_K8S_JOB_STORAGE_SIZE
            if graphnet_config.GRAPHNET_K8S_USE_LOCAL_STORAGE:
                host_path = {"path": f"/tmp/{str(int(time.time() * 10 ** 6))}"}
                if graphnet_config.GRAPHNET_K8S_USE_SINGLE_WORKDIR_STORAGE:
                    work_pv = k8s.convert_k8s_name("opensafely-workdir", "pv")
                else:
                    work_pv = k8s.convert_k8s_name(job.workspace, "pv")
                
                job_pv = k8s.convert_k8s_name(job.id, "pv")
                k8s.create_pv(work_pv, storage_class, ws_pv_size, host_path, get_app_labels())
                k8s.create_pv(job_pv, storage_class, job_pv_size, host_path, get_app_labels())
                
                access_mode = "ReadWriteOnce"
                k8s.create_pvc(work_pv, work_pvc, storage_class, namespace, ws_pv_size, access_mode, get_app_labels())
                k8s.create_pvc(job_pv, job_pvc, storage_class, namespace, job_pv_size, access_mode, get_app_labels())
            else:
                # 4. Create an ephemeral workspace to use for executing this job. This is expected to be a volume mounted into the container,
                # but other implementations are allowed.
                access_mode = "ReadWriteMany"
                k8s.create_pvc(None, work_pvc, storage_class, namespace, ws_pv_size, access_mode, get_app_labels())
                k8s.create_pvc(None, job_pvc, storage_class, namespace, job_pv_size, access_mode, get_app_labels())
            
            commit_sha = job.study.commit
            inputs = ";".join(job.inputs)
            repo_url = job.study.git_repo_url
            
            # 5. Launch a prepare task asynchronously. If launched successfully, return the PREPARING state. If not, return an ERROR state with message.
            workspace_dir = get_high_privacy_workspace_dir(job.workspace)
            private_repo_access_token = config.PRIVATE_REPO_ACCESS_TOKEN
            app_label = get_opensafely_job_pod_label(job)
            prepare(prepare_job_name, commit_sha, workspace_dir, inputs, job_pvc, private_repo_access_token, repo_url, work_pvc, app_label)
            
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
            if not k8s.is_pvc_created(job_pvc):
                return JobStatus(ExecutorState.ERROR, f"PVC not found {job_pvc}")
            
            # 3. Check there are resources availabe to execute the job. If not, return PREPARED status with an appropriate message.
            execute_job_name = get_execute_job_name(job)
            execute_job_arg = job.args
            execute_job_command = None
            execute_job_env = dict(job.env)
            execute_job_image = job.image
            
            # add DB URL for cohortextractor
            if job.allow_database_access:
                execute_job_env['DATABASE_URL'] = config.DATABASE_URLS['full']
            
            namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
            whitelist = graphnet_config.GRAPHNET_K8S_EXECUTION_HOST_WHITELIST
            if job.allow_database_access and len(whitelist.strip()) > 0:
                network_labels = k8s.create_network_policy(namespace, [ip_port.split(":") for ip_port in whitelist.split(",")])  # allow whitelist
            else:
                network_labels = k8s.create_network_policy(namespace, [])  # deny all
            
            # 4. Launch the job execution task asynchronously. If launched successfully, return the EXECUTING state. If not, return an ERROR state with message.
            app_label = get_opensafely_job_pod_label(job)
            execute(execute_job_name, execute_job_arg, execute_job_command, execute_job_env, execute_job_image, job_pvc, network_labels, app_label)
            
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
            if not k8s.is_pvc_created(job_pvc):
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
            
            finalize_job_name = k8s.convert_k8s_name(opensafely_job_name, "finalize", additional_hash=opensafely_job_id)
            app_label = get_opensafely_job_pod_label(job)
            finalize(finalize_job_name, action, execute_job_name, job_pvc, output_spec, work_pvc, workspace_name, job, app_label)
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
        prepare_state = k8s.read_k8s_job_status(prepare_job_name, namespace)
        if prepare_state == k8s.K8SJobStatus.UNKNOWN:
            return JobStatus(ExecutorState.UNKNOWN)
        elif prepare_state == K8SJobStatus.PENDING or prepare_state == K8SJobStatus.RUNNING:
            return JobStatus(ExecutorState.PREPARING)
        elif prepare_state == K8SJobStatus.FAILED:
            try:
                logs = k8s.read_log(prepare_job_name, namespace)
                return JobStatus(ExecutorState.ERROR, json.dumps(logs.values()))
            except Exception as e:
                return JobStatus(ExecutorState.ERROR, str(e))
        elif prepare_state == K8SJobStatus.SUCCEEDED:
            
            execute_job_name = get_execute_job_name(job)
            execute_state = k8s.read_k8s_job_status(execute_job_name, namespace)
            if execute_state == K8SJobStatus.UNKNOWN:
                return JobStatus(ExecutorState.PREPARED)
            elif execute_state == K8SJobStatus.PENDING or execute_state == K8SJobStatus.RUNNING:
                return JobStatus(ExecutorState.EXECUTING)
            elif execute_state == K8SJobStatus.FAILED:
                try:
                    logs = k8s.read_log(execute_job_name, namespace)
                    return JobStatus(ExecutorState.ERROR, json.dumps(logs.values()))
                except Exception as e:
                    return JobStatus(ExecutorState.ERROR, str(e))
            elif execute_state == K8SJobStatus.SUCCEEDED:
                
                finalize_job_name = get_finalize_job_name(job)
                finalize_state = k8s.read_k8s_job_status(finalize_job_name, namespace)
                if finalize_state == K8SJobStatus.UNKNOWN:
                    return JobStatus(ExecutorState.EXECUTED)
                elif finalize_state == K8SJobStatus.PENDING or finalize_state == K8SJobStatus.RUNNING:
                    return JobStatus(ExecutorState.FINALIZING)
                elif finalize_state == K8SJobStatus.FAILED:
                    try:
                        logs = k8s.read_log(finalize_job_name, namespace)
                        return JobStatus(ExecutorState.ERROR, json.dumps(logs.values()))
                    except Exception as e:
                        return JobStatus(ExecutorState.ERROR, str(e))
                elif finalize_state == K8SJobStatus.SUCCEEDED:
                    return JobStatus(ExecutorState.FINALIZED)
        
        # should not happen
        return JobStatus(ExecutorState.ERROR, "Unknown status found in get_status()")
    
    def terminate(self, job: JobDefinition) -> JobStatus:
        # 1. If any task for this job is running, terminate it, do not wait for it to complete.
        jobs_deleted = delete_all_jobs(job, keep_failed=graphnet_config.GRAPHNET_K8S_KEEP_FAILED_JOB)
        
        # 2. Return ERROR state with a message.
        return JobStatus(ExecutorState.ERROR, f"deleted {','.join(jobs_deleted)}")
    
    def cleanup(self, job: JobDefinition) -> JobStatus:
        # 1. Initiate the cleanup, do not wait for it to complete.
        namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
        
        delete_all_jobs(job, keep_failed=graphnet_config.GRAPHNET_K8S_KEEP_FAILED_JOB)
        
        job_pvc = get_job_pvc_name(job)
        job_pv = k8s.read_pv_name(namespace, job_pvc)  # read the pv bound to the pvc instead of the generated name
        
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
        container_name = k8s.JOB_CONTAINER_NAME
        
        exit_code = k8s.read_container_exit_code(job_name, container_name, namespace)
        
        image_id = k8s.read_image_id(job_name, container_name, namespace)
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
            work_pvc = k8s.convert_k8s_name(workspace, "pvc")
            
            job_name = delete_work_files(workspace, privacy, paths, work_pvc, namespace)
            status = k8s.await_job_status(job_name, namespace)
            if status != K8SJobStatus.SUCCEEDED:
                raise Exception(f"unable to delete_files {workspace} {privacy} {paths} {job_name}")
        except Exception as e:
            log.exception(e)


def prepare(prepare_job_name, commit_sha, workspace_dir, inputs, job_pvc, private_repo_access_token, repo_url, work_pvc, additional_labels=None):
    repos_root = WORK_DIR + "/repos"
    command = ['python', '-m', 'jobrunner.prepare']
    args = [repo_url, commit_sha, repos_root, str(workspace_dir), JOB_DIR, inputs]
    env = {'PRIVATE_REPO_ACCESS_TOKEN': private_repo_access_token}
    storages = [
        (work_pvc, WORK_DIR, False),
        (job_pvc, JOB_DIR, True),
    ]
    image_pull_policy = graphnet_config.GRAPHNET_K8S_IMAGE_PULL_POLICY
    namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
    tool_image = graphnet_config.GRAPHNET_K8S_JOB_RUNNER_TOOL_IMAGE
    
    labels = dict()
    if additional_labels:
        labels.update(additional_labels)
    
    job_labels = get_app_labels()
    
    k8s.create_k8s_job(prepare_job_name, namespace, tool_image, command, args, env, storages, job_labels, labels, image_pull_policy=image_pull_policy)
    return prepare_job_name


def execute(execute_job_name, execute_job_arg, execute_job_command, execute_job_env, execute_job_image, job_pvc, network_labels, additional_labels=None):
    command = execute_job_command
    args = execute_job_arg
    storages = [
        (job_pvc, JOB_DIR, True),
    ]
    env = execute_job_env
    image_pull_policy = graphnet_config.GRAPHNET_K8S_IMAGE_PULL_POLICY
    namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
    
    labels = dict()
    if network_labels:
        labels.update(network_labels)
    if additional_labels:
        labels.update(additional_labels)
    
    job_labels = get_app_labels()
    
    k8s.create_k8s_job(execute_job_name, namespace, execute_job_image, command, args, env, storages, job_labels, labels, image_pull_policy=image_pull_policy)
    return execute_job_name


def finalize(finalize_job_name, action, execute_job_name, job_pvc, output_spec, work_pvc, workspace_name, job_definition: JobDefinition = None, additional_labels=None):
    namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
    
    high_privacy_storage_base = Path(WORK_DIR) / "high_privacy"
    high_privacy_workspace_dir = get_high_privacy_workspace_dir(workspace_name)
    high_privacy_metadata_dir = high_privacy_workspace_dir / "metadata"
    high_privacy_log_dir = high_privacy_storage_base / 'logs' / datetime.date.today().strftime("%Y-%m") / execute_job_name
    high_privacy_action_log_path = high_privacy_metadata_dir / f"{action}.log"
    
    medium_privacy_storage_base = Path(WORK_DIR) / "medium_privacy"
    medium_privacy_workspace_dir = medium_privacy_storage_base / 'workspaces' / workspace_name
    medium_privacy_metadata_dir = medium_privacy_workspace_dir / "metadata"
    
    output_spec_json = json.dumps(output_spec)
    job_definition_map_json = json.dumps(dataclasses.asdict(job_definition) if job_definition else dict())
    
    use_local_k8s_config = False  # must be false in order to allow a pod to access resource in the cluster
    
    command = ['python', '-m', 'jobrunner.finalize']
    args = [JOB_DIR, high_privacy_workspace_dir, high_privacy_metadata_dir, high_privacy_log_dir, high_privacy_action_log_path, medium_privacy_workspace_dir,
            medium_privacy_metadata_dir, output_spec_json, use_local_k8s_config, execute_job_name, namespace, job_definition_map_json]
    args = [str(a) for a in args]
    env = {}
    storages = [
        (work_pvc, WORK_DIR, False),
        (job_pvc, JOB_DIR, True),
    ]
    image_pull_policy = graphnet_config.GRAPHNET_K8S_IMAGE_PULL_POLICY
    tool_image = graphnet_config.GRAPHNET_K8S_JOB_RUNNER_TOOL_IMAGE
    
    labels = dict()
    if additional_labels:
        labels.update(additional_labels)
    
    job_labels = get_app_labels()
    
    k8s.create_k8s_job(finalize_job_name, namespace, tool_image, command, args, env, storages, job_labels, labels, image_pull_policy=image_pull_policy,
                       service_account_name=graphnet_config.GRAPHNET_K8S_JOB_SERVICE_ACCOUNT)
    
    return finalize_job_name


def delete_work_files(workspace, privacy, paths, work_pvc, namespace, additional_labels=None):
    job_name = k8s.convert_k8s_name(workspace, f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}-delete-job", additional_hash=";".join(paths))
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
    image_pull_policy = graphnet_config.GRAPHNET_K8S_IMAGE_PULL_POLICY
    
    labels = dict()
    if additional_labels:
        labels.update(additional_labels)
    
    job_labels = get_app_labels()
    
    k8s.create_k8s_job(job_name, namespace, image, command, args, {}, storage, job_labels, labels, image_pull_policy=image_pull_policy)
    return job_name


def delete_all_jobs(job, keep_failed=False):
    namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
    
    jobs_deleted = []
    
    # pods = k8s.list_pod_with_label(namespace, OPENSAFELY_LABEL_TAG, get_opensafely_job_label(job))
    # for pod in pods:
    #     try:
    #         pod_name = pod.metadata.name
    #         job_name = pod.metadata.labels.get('job-name')
    #         core_v1.delete_namespaced_pod(pod_name, namespace)
    #
    #         if job_name:
    #             batch_v1.delete_namespaced_job(job_name, namespace)
    #             jobs_deleted.append(job_name)
    #     except:  # already deleted
    #         pass
    
    prepare_job_name = get_prepare_job_name(job)
    deleted = delete_job(prepare_job_name, namespace, keep_failed)
    if deleted:
        jobs_deleted.append(prepare_job_name)
    
    execute_job_name = get_execute_job_name(job)
    deleted = delete_job(execute_job_name, namespace, keep_failed)
    if deleted:
        jobs_deleted.append(execute_job_name)
    
    finalize_job_name = get_finalize_job_name(job)
    deleted = delete_job(finalize_job_name, namespace, keep_failed)
    if deleted:
        jobs_deleted.append(finalize_job_name)
    
    return jobs_deleted


def delete_job(job_name, namespace, keep_failed) -> bool:
    try:
        if keep_failed:
            job_status = k8s.read_k8s_job_status(job_name, namespace)
            if job_status == K8SJobStatus.FAILED:
                return False
        k8s.delete_job(job_name, namespace)
        return True
    except:  # already deleted
        return False


def read_finalize_output(opensafely_job_name, opensafely_job_id, namespace):
    finalize_job_name = k8s.convert_k8s_name(opensafely_job_name, "finalize", additional_hash=opensafely_job_id)
    logs = k8s.read_log(finalize_job_name, namespace)
    container_log = ""
    for (_, container_name), container_log in logs.items():
        if container_name == k8s.JOB_CONTAINER_NAME:
            break
    for line in container_log.split('\n'):
        if line.startswith(JOB_RESULTS_TAG):
            job_result = line[len(JOB_RESULTS_TAG):]
            return json.loads(job_result)
    return None


def get_high_privacy_workspace_dir(workspace_name):
    return Path(WORK_DIR) / "high_privacy" / 'workspaces' / workspace_name


def get_app_labels():
    return {
        "app": "job-executor"
    }


def get_opensafely_job_pod_label(job: JobDefinition) -> dict:
    return {
        OPENSAFELY_LABEL_TAG: k8s.convert_k8s_name(get_opensafely_job_name(job), additional_hash=job.id),
        "app"               : "job-executor"
    }


def get_job_pvc_name(job):
    return k8s.convert_k8s_name(job.id, "pvc")


def get_work_pvc_name(job):
    if graphnet_config.GRAPHNET_K8S_USE_SINGLE_WORKDIR_STORAGE:
        return k8s.convert_k8s_name("opensafely-workdir", "pvc")
    else:
        return k8s.convert_k8s_name(job.workspace, "pvc")


def get_opensafely_job_name(job):
    return f"{job.workspace}_{job.action}"


def get_execute_job_name(job):
    return k8s.convert_k8s_name(get_opensafely_job_name(job), "execute", additional_hash=job.id)


def get_prepare_job_name(job):
    return k8s.convert_k8s_name(get_opensafely_job_name(job), "prepare", additional_hash=job.id)


def get_finalize_job_name(job):
    return k8s.convert_k8s_name(get_opensafely_job_name(job), "finalize", additional_hash=job.id)
