import configparser
import datetime
import os
import random
import re
import shlex
import subprocess
import time

import pytest

from jobrunner import config
from jobrunner.executors.graphnet import k8s
from jobrunner.executors.graphnet.k8s import K8SJobStatus
from jobrunner.executors.graphnet.k8s_executor import (
    WORK_DIR,
    K8SExecutorAPI,
    get_work_pvc_name,
    get_job_pvc_name
)
from jobrunner.job_executor import *

NAMESPACE = "opensafely-unittest"

K8S_TEST_CONFIG_FILE = "private_test_config.ini"
# K8S_TEST_CONFIG_FILE = "private_test_config_remote.ini"

private_config = configparser.RawConfigParser()
private_config.read(K8S_TEST_CONFIG_FILE)

IMAGE_PULL_POLICY = private_config.get('config', 'IMAGE_PULL_POLICY', fallback="Never")
TOOLS_IMAGE = private_config.get('config', 'TOOLS_IMAGE', fallback="opensafely-job-runner-tools:latest")
COHORT_EXTRACTOR_IMAGE = private_config.get('config', 'COHORT_EXTRACTOR_IMAGE', fallback='ghcr.io/opensafely-core/cohortextractor:latest')
STORAGE_CLASS = private_config.get('config', 'STORAGE_CLASS', fallback="standard")
PRIVATE_REPO_ACCESS_TOKEN = private_config.get('git', 'PRIVATE_REPO_ACCESS_TOKEN', fallback='')
USE_LOCAL_STORAGE = private_config.get('config', 'USE_LOCAL_STORAGE', fallback='1')
DB_URLS = {
    'full': private_config.get('config', 'DB_URL', fallback='mssql://dummy_user:dummy_password@127.0.0.1:1433/dummy_db')
}


@pytest.fixture(params=[
    ("job", None),
    ("job+++++++++-+++++++++-+++++++++-+++++++++-+++++++++-+++++++++-+++++++++-+++++++++-+++++++++-test", None),
    ("abcdefghi-abcdefghi-abcdefghi-abcdefghi-abcdefghi-abcde", None),  # 55+7+1 = 63 char
    ("abcdefghi-abcdefghi-abcdefghi-abcdefghi-abcdefghi-abcdef", None),  # 56+7+1 = 64 char
    ("1job1", None),
    ("-job-", None),
    ("123job-", None),
    ("job-+++", None),
    ("job", "1job1"),
    ("job", "-job-"),
    ("job", "123job-"),
    ("job", "job-+++"),
])
def k8s_names(request):
    job_name, suffix = request.param
    return job_name, suffix


def test_convert_k8s_name(k8s_names):
    job_name, suffix = k8s_names
    
    result = k8s.convert_k8s_name(job_name, suffix)
    print(job_name, suffix, result)
    
    # contain at most 63 characters
    assert len(result) <= 63
    
    # contain only lowercase alphanumeric characters or '-'
    assert len(re.findall(r'[^a-z0-9-]', result)) == 0
    
    # start with an alphabetic character
    assert re.match(r'[a-z]', result[0])
    
    # end with an alphanumeric character
    assert re.match(r'[a-z0-9]', result[-1])


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
@pytest.mark.skipif(not os.path.exists(K8S_TEST_CONFIG_FILE), reason="no k8s config found")
def test_generate_cohort_with_JobAPI(monkeypatch):
    namespace = NAMESPACE
    
    monkeypatch.setattr("jobrunner.config.DEBUG", 1)
    monkeypatch.setattr("jobrunner.config.DATABASE_URLS", DB_URLS)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", True)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_STORAGE", USE_LOCAL_STORAGE == '1')
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_RUNNER_TOOL_IMAGE", TOOLS_IMAGE)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_WS_STORAGE_SIZE", "100M")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_STORAGE_SIZE", "100M")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_EXECUTION_HOST_WHITELIST", "127.0.0.1:1433")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_STORAGE_CLASS", STORAGE_CLASS)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_IMAGE_PULL_POLICY", IMAGE_PULL_POLICY)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_SERVICE_ACCOUNT", "job-runner-unittest-account")
    
    account_file = os.path.join(os.path.dirname(__file__), "service_accounts.yaml")
    cmd = f"kubectl create ns {namespace} && kubectl -n {namespace} apply -f {account_file}"
    print(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    for line in iter(p.stdout.readline, b''):
        print(line)
    
    workspace_name = "test_workspace"
    opensafely_job_id = "test_job_id"
    created_at = int(time.time())
    repo_url = "https://github.com/graphnet-opensafely/opensafely-SRO-Measures.git"
    commit_sha = "8cfdfbaadbc63c7b5023609731f4a591e3e279fa"
    config.PRIVATE_REPO_ACCESS_TOKEN = PRIVATE_REPO_ACCESS_TOKEN
    inputs = []
    output_spec = {'output/input_*.csv': 'highly_sensitive'}
    
    allow_network_access = True
    execute_job_image = COHORT_EXTRACTOR_IMAGE
    action = 'generate_study_population'
    execute_job_arg = ['generate_cohort', '--study-definition', 'study_definition', '--index-date-range',
                       '2021-01-01 to 2021-02-01 by month', '--output-dir=output', '--output-dir=output', '--expectations-population=1']
    execute_job_env = {'OPENSAFELY_BACKEND': 'graphnet'}
    
    job = JobDefinition(
            opensafely_job_id,
            opensafely_job_id,
            Study(repo_url, commit_sha),
            workspace_name,
            action,
            created_at,
            execute_job_image,
            execute_job_arg,
            execute_job_env,
            inputs,
            output_spec,
            allow_network_access
    )
    
    job_api = K8SExecutorAPI()
    status = job_api.get_status(job)
    assert status.state == ExecutorState.UNKNOWN
    
    job_api.prepare(job)
    status = job_api.get_status(job)
    assert status.state == ExecutorState.PREPARING
    while True:
        status = job_api.get_status(job)
        if status.state != ExecutorState.PREPARING:
            break
        time.sleep(1)
    assert status.state == ExecutorState.PREPARED
    
    job_api.execute(job)
    status = job_api.get_status(job)
    assert status.state == ExecutorState.EXECUTING
    while True:
        status = job_api.get_status(job)
        if status.state != ExecutorState.EXECUTING:
            break
        time.sleep(1)
    assert status.state == ExecutorState.EXECUTED
    
    job_api.finalize(job)
    status = job_api.get_status(job)
    assert status.state == ExecutorState.FINALIZING
    while True:
        status = job_api.get_status(job)
        if status.state != ExecutorState.FINALIZING:
            break
        time.sleep(1)
    assert status.state == ExecutorState.FINALIZED
    
    results = job_api.get_results(job)
    assert results.outputs == {'output/input_2021-01-01.csv': 'highly_sensitive', 'output/input_2021-02-01.csv': 'highly_sensitive'}
    assert results.unmatched_patterns == []
    assert results.exit_code == 0
    assert results.image_id is not None
    
    # check if the file is saved in the work pv
    work_pvc = get_work_pvc_name(job)
    result = list_files_in_volume(namespace, work_pvc, WORK_DIR)
    for r in result:
        print(r)
    assert f'/workdir/high_privacy/workspaces/{workspace_name}/output/input_2021-01-01.csv' in result
    assert f'/workdir/high_privacy/workspaces/{workspace_name}/output/input_2021-02-01.csv' in result
    assert f'/workdir/high_privacy/workspaces/{workspace_name}/metadata/generate_study_population.log' in result
    
    from jobrunner.run import get_obsolete_files
    obsolete = get_obsolete_files(job, results.outputs)
    job_api.delete_files(job.workspace, Privacy.HIGH, obsolete)
    job_api.delete_files(job.workspace, Privacy.MEDIUM, obsolete)
    
    job_api.cleanup(job)
    
    # clean up
    work_pv = k8s.read_pv_name(namespace, get_work_pvc_name(job))
    job_pv = k8s.read_pv_name(namespace, get_job_pvc_name(job))
    
    delete_namespace(namespace)
    
    delete_persistent_volume(work_pv)
    delete_persistent_volume(job_pv)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
@pytest.mark.skipif(not os.path.exists(K8S_TEST_CONFIG_FILE), reason="no k8s config found")
def test_job_env(monkeypatch):
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", True)
    
    namespace = NAMESPACE
    
    k8s.init_k8s_config(True)
    
    k8s.create_namespace(namespace)
    
    ids = list(range(5))
    random.shuffle(ids)
    
    job = "test-job1"
    image = "busybox"
    command = ['/bin/sh', '-c']
    args = ["echo job; printenv; sleep 3"]
    
    env = {}
    for i in range(10):
        env[f'test{i}'] = str(random.randint(0, 100))
    
    storage = []
    pod_labels = dict()
    k8s.create_k8s_job(job, namespace, image, command, args, env, storage, get_app_labels(), pod_labels, image_pull_policy=IMAGE_PULL_POLICY)
    
    # assert
    status = k8s.await_job_status(job, namespace)
    logs = k8s.read_log(job, namespace)
    print(logs)
    assert status == K8SJobStatus.SUCCEEDED
    logs = list(logs.values())[0]
    for k, v in env.items():
        assert f"{k}={v}" in logs
    
    # clean up
    delete_namespace(namespace)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
@pytest.mark.skipif(not os.path.exists(K8S_TEST_CONFIG_FILE), reason="no k8s config found")
def test_job_network(monkeypatch):
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", 1)
    
    k8s.init_k8s_config(True)
    
    namespace = NAMESPACE
    k8s.create_namespace(namespace)
    
    github_network_labels = k8s.create_network_policy(namespace, [('github-proxy.opensafely.org', '80')])
    deny_all_network_labels = k8s.create_network_policy(namespace, [])
    
    jobs = []
    
    job_allowed = "os-job-with-policy"
    image = "curlimages/curl"
    command = ['/bin/sh', '-c']
    args = ["curl --request GET http://157.245.31.108 --max-time 3"]  # github-proxy.opensafely.org
    storage = []
    k8s.create_k8s_job(job_allowed, namespace, image, command, args, {}, storage, get_app_labels(), github_network_labels, image_pull_policy=IMAGE_PULL_POLICY)
    jobs.append(job_allowed)
    
    job_blocked = "os-job-no-policy"
    image = "curlimages/curl"
    command = ['/bin/sh', '-c']
    args = ["curl --request GET http://157.245.31.108 --max-time 3"]  # github-proxy.opensafely.org
    storage = []
    k8s.create_k8s_job(job_blocked, namespace, image, command, args, {}, storage, get_app_labels(), deny_all_network_labels, image_pull_policy=IMAGE_PULL_POLICY)
    jobs.append(job_blocked)
    
    # job_domain = "os-job2"
    # image = "curlimages/curl"
    # command = ['/bin/sh', '-c']
    # # args = ["resp=$(curl --request GET https://github-proxy.opensafely.org); echo ${resp:0:100};"]
    # args = ["curl --request GET http://github-proxy.opensafely.org"]
    # storage = []
    # create_k8s_job(job_domain, namespace, image, command, args, {}, storage, network_labels, image_pull_policy=IMAGE_PULL_POLICY)
    # jobs.append(job_domain)
    #
    # job_google = "os-job3"
    # image = "curlimages/curl"
    # command = ['/bin/sh', '-c']
    # # args = ["resp=$(curl --request GET https://www.google.com); echo ${resp:0:100};"]
    # args = ["curl --request GET https://www.google.com"]
    # storage = []
    # create_k8s_job(job_google, namespace, image, command, args, {}, storage, network_labels, image_pull_policy=IMAGE_PULL_POLICY)
    # jobs.append(job_google)
    
    status1 = k8s.await_job_status(job_allowed, namespace)
    status2 = k8s.await_job_status(job_blocked, namespace)
    
    for job_name in jobs:
        log_k8s_job(job_name, namespace)
    
    assert status1 == K8SJobStatus.SUCCEEDED
    assert status2 == K8SJobStatus.FAILED
    
    # clean up
    delete_namespace(namespace)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
@pytest.mark.skipif(not os.path.exists(K8S_TEST_CONFIG_FILE), reason="no k8s config found")
def test_job_sequence(monkeypatch):
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", True)
    
    namespace = NAMESPACE
    pv_name = "job-pv"
    pvc_name = "job-pvc"
    storage_class = STORAGE_CLASS
    size = "100M"
    
    k8s.init_k8s_config(True)
    
    k8s.create_namespace(namespace)
    host_path = {"path": f"/tmp/{str(int(time.time() * 10 ** 6))}"}
    k8s.create_pv(pv_name, storage_class, size, host_path, get_app_labels())
    k8s.create_pvc(pv_name, pvc_name, storage_class, namespace, size, "ReadWriteOnce", get_app_labels())
    
    jobs = []
    
    ids = list(range(5))
    random.shuffle(ids)
    
    for i in ids:
        job = f"test-job{i}"
        image = "busybox"
        command = ['/bin/sh', '-c']
        args = [f'echo job{i}; ls -a /ws;']
        storage = [
            # pvc, path, is_control
            (pvc_name, 'ws', True)
        ]
        depends_on = f"test-job{i - 1}" if i > 0 else None
        k8s.create_k8s_job(job, namespace, image, command, args, {}, storage, get_app_labels(), dict(), depends_on=depends_on, image_pull_policy=IMAGE_PULL_POLICY,
                           use_dependency=True)
        jobs.append(job)
    
    from kubernetes import client
    
    batch_v1 = client.BatchV1Api()
    
    jobs = sorted(jobs)
    
    last_completion_time = None
    for job_name in jobs:
        status = k8s.await_job_status(job_name, namespace)
        log_k8s_job(job_name, namespace)
        assert status == K8SJobStatus.SUCCEEDED
        
        status = batch_v1.read_namespaced_job(job_name, namespace=namespace).status
        if last_completion_time:
            assert last_completion_time < status.completion_time
        last_completion_time = status.completion_time
    
    # clean up
    delete_namespace(namespace)
    delete_persistent_volume(pv_name)


def list_files_in_volume(namespace, pvc_name, path):
    job_name = k8s.convert_k8s_name(f"ls-{pvc_name}-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}", f"job")
    image = "busybox"
    command = ['/bin/sh', '-c']
    args = [f'find {path}']
    storage = [
        # pvc, path, is_control
        (pvc_name, path, False)
    ]
    k8s.create_k8s_job(job_name, namespace, image, command, args, {}, storage, get_app_labels(), dict(), image_pull_policy=IMAGE_PULL_POLICY)
    
    k8s.await_job_status(job_name, namespace)
    outputs = list(k8s.read_log(job_name, namespace).values())[0]
    
    k8s.delete_job(job_name, namespace)
    return outputs.split('\n')


def delete_persistent_volume(pv_name):
    from kubernetes import client
    
    core_v1 = client.CoreV1Api()
    
    try:
        core_v1.delete_persistent_volume(pv_name)
    except:
        # already deleted
        pass
    
    while pv_name in [pv.metadata.name for pv in core_v1.list_persistent_volume().items]:
        time.sleep(.5)


def delete_namespace(namespace):
    from kubernetes import client
    
    core_v1 = client.CoreV1Api()
    
    try:
        core_v1.delete_namespace(namespace)
    except:
        # already deleted
        pass
    
    while namespace in [ns.metadata.name for ns in core_v1.list_namespace().items]:
        time.sleep(.5)


def log_k8s_job(job_name: str, namespace: str):
    print("-" * 10, "start of log", job_name, "-" * 10, "\n")
    logs = k8s.read_log(job_name, namespace)
    for (pod_name, c), log in logs.items():
        print(f"--Log {pod_name}/{c} start:")
        print(log)
        print(f"--Log {pod_name}/{c} end\n")
    
    print("-" * 10, "end of log", job_name, "-" * 10, "\n")


def get_app_labels():
    return {
        "app": "opensafely-unittest"
    }
