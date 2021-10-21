import datetime
import os
import random
import re
import time

import pytest

from jobrunner import config
from jobrunner.job_executor import *
from jobrunner.executors.graphnet.k8s_runner import (
    delete_work_files,
    WORK_DIR,
    K8SJobAPI,
    get_work_pv_name,
    get_job_pv_name,
    get_work_pvc_name
)
from jobrunner.executors.graphnet.k8s import (
    init_k8s_config,
    convert_k8s_name,
    create_k8s_job,
    read_log,
    create_pv,
    create_pvc,
    create_namespace,
    create_network_policy,
    K8SJobStatus,
    read_finalize_output,
    read_image_id,
    JOB_CONTAINER_NAME,
    await_job_status,
)

K8S_TEST_CONFIG_FILE = "private_test_config.ini"


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
    
    result = convert_k8s_name(job_name, suffix)
    print(job_name, suffix, result)
    
    # contain at most 63 characters
    assert len(result) <= 63
    
    # contain only lowercase alphanumeric characters or '-'
    assert len(re.findall(r'[^a-z0-9-]', result)) == 0
    
    # start with an alphabetic character
    assert re.match(r'[a-z]', result[0])
    
    # end with an alphanumeric character
    assert re.match(r'[a-z0-9]', result[-1])


def list_files_in_volume(namespace, pvc_name, path):
    job_name = convert_k8s_name(f"ls-{pvc_name}-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}", f"job")
    image = "busybox"
    command = ['/bin/sh', '-c']
    args = [f'find {path}']
    storage = [
        # pvc, path, is_control
        (pvc_name, path, False)
    ]
    create_k8s_job(job_name, namespace, image, command, args, {}, storage, dict(), image_pull_policy="Never")
    
    await_job_status(job_name, namespace)
    outputs = list(read_log(job_name, namespace).values())[0]
    return outputs.split('\n')


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
@pytest.mark.skipif(not os.path.exists(K8S_TEST_CONFIG_FILE), reason="no k8s config found")
def test_generate_cohort_with_JobAPI(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.config.DEBUG", 1)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner-tools:latest")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_WS_STORAGE_SIZE", "100M")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_STORAGE_SIZE", "100M")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_EXECUTION_HOST_WHITELIST", "127.0.0.1:1433")
    
    import configparser
    private_config = configparser.RawConfigParser()
    private_config.read('private_test_config.ini')
    
    workspace_name = "test_workspace"
    opensafely_job_id = "test_job_id"
    repo_url = "https://github.com/graphnet-opensafely/opensafely-SRO-Measures.git"
    commit_sha = "8cfdfbaadbc63c7b5023609731f4a591e3e279fa"
    config.PRIVATE_REPO_ACCESS_TOKEN = private_config.get('git', 'PRIVATE_REPO_ACCESS_TOKEN')
    inputs = []
    output_spec = {'output/input_*.csv': 'highly_sensitive'}
    
    allow_network_access = True
    execute_job_image = 'ghcr.io/opensafely-core/cohortextractor:latest'
    execute_job_arg = ['generate_cohort', '--study-definition', 'study_definition', '--index-date-range', '2021-01-01 to 2021-02-01 by month', '--output-dir=output',
                       '--output-dir=output', '--expectations-population=1']
    execute_job_env = {'OPENSAFELY_BACKEND': 'graphnet', 'DATABASE_URL': 'mssql://dummy_user:dummy_password@127.0.0.1:1433/dummy_db'}
    
    job = JobDefinition(
            opensafely_job_id,
            Study(repo_url, commit_sha),
            workspace_name,
            execute_job_arg[0],
            execute_job_image,
            execute_job_arg[1:],
            execute_job_env,
            inputs,
            output_spec,
            allow_network_access
    )
    
    job_api = K8SJobAPI()
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
    assert f'/workdir/high_privacy/workspaces/{workspace_name}/metadata/generate_cohort.log' in result
    
    job_api.cleanup(job)
    
    # clean up
    delete_namespace(namespace)
    
    work_pv = get_work_pv_name(job)
    job_pv = get_job_pv_name(job)
    delete_persistent_volume(work_pv)
    delete_persistent_volume(job_pv)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
@pytest.mark.skipif(not os.path.exists(K8S_TEST_CONFIG_FILE), reason="no k8s config found")
def test_generate_cohort_old(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner-tools:latest")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_WS_STORAGE_SIZE", "100M")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_STORAGE_SIZE", "100M")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_EXECUTION_HOST_WHITELIST", "127.0.0.1:1433")
    
    init_k8s_config(True)
    
    import configparser
    private_config = configparser.RawConfigParser()
    private_config.read('private_test_config.ini')
    
    workspace_name = "test_workspace"
    opensafely_job_id = "test_job_id"
    opensafely_job_name = "test_job_name"
    repo_url = "https://github.com/graphnet-opensafely/opensafely-SRO-Measures.git"
    commit_sha = "8cfdfbaadbc63c7b5023609731f4a591e3e279fa"
    private_repo_access_token = private_config.get('git', 'PRIVATE_REPO_ACCESS_TOKEN')
    inputs = ""
    output_spec = {'output/input_*.csv': 'highly_sensitive'}
    
    allow_network_access = True
    execute_job_image = 'ghcr.io/opensafely-core/cohortextractor:latest'
    execute_job_command = None
    execute_job_arg = ['generate_cohort', '--study-definition', 'study_definition', '--index-date-range', '2021-01-01 to 2021-02-01 by month', '--output-dir=output',
                       '--output-dir=output', '--expectations-population=1']
    execute_job_env = {'OPENSAFELY_BACKEND': 'graphnet', 'DATABASE_URL': 'mssql://dummy_user:dummy_password@127.0.0.1:1433/dummy_db'}
    
    jobs, work_pv, work_pvc, job_pv, _ = create_opensafely_job(workspace_name, opensafely_job_id, opensafely_job_name, repo_url, private_repo_access_token, commit_sha,
                                                               inputs,
                                                               allow_network_access, execute_job_image, execute_job_command, execute_job_arg, execute_job_env,
                                                               output_spec)
    
    for job_name in jobs:
        status = await_job_status(job_name, namespace)
        log_k8s_job(job_name, namespace)
        assert status == K8SJobStatus.SUCCEEDED
    
    job_status = read_finalize_output(opensafely_job_name, opensafely_job_id, namespace)
    print(job_status)
    assert job_status == {'outputs': {'output/input_2021-01-01.csv': 'highly_sensitive', 'output/input_2021-02-01.csv': 'highly_sensitive'}, 'unmatched': []}
    
    execute_job_name = jobs[1]
    image_id = read_image_id(execute_job_name, JOB_CONTAINER_NAME, namespace)
    print(image_id)
    assert image_id is not None
    
    result = list_files_in_volume(namespace, work_pvc, WORK_DIR)
    for r in result:
        print(r)
    assert f'/workdir/high_privacy/workspaces/{workspace_name}/output/input_2021-01-01.csv' in result
    assert f'/workdir/high_privacy/workspaces/{workspace_name}/output/input_2021-02-01.csv' in result
    
    delete_job_name = delete_work_files(workspace_name, Privacy.HIGH, ['output/input_2021-01-01.csv'], work_pvc, namespace)
    status = await_job_status(delete_job_name, namespace)
    log_k8s_job(delete_job_name, namespace)
    assert status == K8SJobStatus.SUCCEEDED
    print()
    
    result = list_files_in_volume(namespace, work_pvc, WORK_DIR)
    for r in result:
        print(r)
    assert f'/workdir/high_privacy/workspaces/{workspace_name}/output/input_2021-01-01.csv' not in result
    assert f'/workdir/high_privacy/workspaces/{workspace_name}/output/input_2021-02-01.csv' in result
    
    # clean up
    delete_namespace(namespace)
    
    delete_persistent_volume(work_pv)
    delete_persistent_volume(job_pv)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
@pytest.mark.skipif(not os.path.exists(K8S_TEST_CONFIG_FILE), reason="no k8s config found")
def test_job_env(monkeypatch):
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", 1)
    
    namespace = "opensafely-test"
    
    init_k8s_config(True)
    
    create_namespace(namespace)
    
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
    create_k8s_job(job, namespace, image, command, args, env, storage, pod_labels, image_pull_policy="Never")
    
    # assert
    status = await_job_status(job, namespace)
    logs = read_log(job, namespace)
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
    
    init_k8s_config(True)
    
    namespace = "opensafely-test"
    create_namespace(namespace)
    
    github_network_labels = create_network_policy(namespace, [('github-proxy.opensafely.org', '80')])
    deny_all_network_labels = create_network_policy(namespace, [])
    
    jobs = []
    
    job_allowed = "os-job-with-policy"
    image = "curlimages/curl"
    command = ['/bin/sh', '-c']
    args = ["curl --request GET http://157.245.31.108 --max-time 3"]  # github-proxy.opensafely.org
    storage = []
    create_k8s_job(job_allowed, namespace, image, command, args, {}, storage, github_network_labels, image_pull_policy="Never")
    jobs.append(job_allowed)
    
    job_blocked = "os-job-no-policy"
    image = "curlimages/curl"
    command = ['/bin/sh', '-c']
    args = ["curl --request GET http://157.245.31.108 --max-time 3"]  # github-proxy.opensafely.org
    storage = []
    create_k8s_job(job_blocked, namespace, image, command, args, {}, storage, deny_all_network_labels, image_pull_policy="Never")
    jobs.append(job_blocked)
    
    # job_domain = "os-job2"
    # image = "curlimages/curl"
    # command = ['/bin/sh', '-c']
    # # args = ["resp=$(curl --request GET https://github-proxy.opensafely.org); echo ${resp:0:100};"]
    # args = ["curl --request GET http://github-proxy.opensafely.org"]
    # storage = []
    # create_k8s_job(job_domain, namespace, image, command, args, {}, storage, network_labels, image_pull_policy="Never")
    # jobs.append(job_domain)
    #
    # job_google = "os-job3"
    # image = "curlimages/curl"
    # command = ['/bin/sh', '-c']
    # # args = ["resp=$(curl --request GET https://www.google.com); echo ${resp:0:100};"]
    # args = ["curl --request GET https://www.google.com"]
    # storage = []
    # create_k8s_job(job_google, namespace, image, command, args, {}, storage, network_labels, image_pull_policy="Never")
    # jobs.append(job_google)
    
    status1 = await_job_status(job_allowed, namespace)
    status2 = await_job_status(job_blocked, namespace)
    
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
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", 1)
    
    namespace = "opensafely-test"
    pv_name = "job-pv"
    pvc_name = "job-pvc"
    storage_class = "standard"
    size = "100M"
    
    init_k8s_config(True)
    
    create_namespace(namespace)
    host_path = {"path": f"/tmp/{str(int(time.time() * 10 ** 6))}"}
    create_pv(pv_name, storage_class, size, host_path)
    create_pvc(pv_name, pvc_name, storage_class, namespace, size)
    
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
        create_k8s_job(job, namespace, image, command, args, {}, storage, dict(), depends_on=depends_on,
                       image_pull_policy="Never")
        jobs.append(job)
    
    from kubernetes import client
    
    batch_v1 = client.BatchV1Api()
    
    jobs = sorted(jobs)
    
    last_completion_time = None
    for job_name in jobs:
        status = await_job_status(job_name, namespace)
        log_k8s_job(job_name, namespace)
        assert status == K8SJobStatus.SUCCEEDED
        
        status = batch_v1.read_namespaced_job(job_name, namespace=namespace).status
        if last_completion_time:
            assert last_completion_time < status.completion_time
        last_completion_time = status.completion_time
    
    # clean up
    delete_namespace(namespace)
    delete_persistent_volume(pv_name)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
@pytest.mark.skipif(not os.path.exists(K8S_TEST_CONFIG_FILE), reason="no k8s config found")
def test_create_concurrent_jobs(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner-tools:latest")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_WS_STORAGE_SIZE", "100M")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_STORAGE_SIZE", "100M")
    
    init_k8s_config(True)
    
    allow_network_access = True
    execute_job_image = 'busybox'
    execute_job_command = ['/bin/sh', '-c']
    execute_job_arg = [f"echo job; ls -R -a /workspace;"]
    execute_job_env = {'OPENSAFELY_BACKEND': 'graphnet', 'DATABASE_URL': 'mssql://dummy_user:dummy_password@dummy_server:1433/dummy_db'}
    
    # same workspace, same name, but different id
    workspace = "test_workspace"
    opensafely_job_name = "test_job_name"
    jobs1, ws_pv_1, ws_pvc_1, job_pv_1, job_pvc_1 = create_opensafely_job(workspace, "test_job_id_1", opensafely_job_name,
                                                                          "https://github.com/opensafely-core/test-public-repository.git", '',
                                                                          "c1ef0e676ec448b0a49e0073db364f36f6d6d078", "", allow_network_access, execute_job_image,
                                                                          execute_job_command, execute_job_arg, execute_job_env, {})
    
    jobs2, ws_pv_2, ws_pvc_2, job_pv_2, job_pvc_2 = create_opensafely_job(workspace, "test_job_id_2", opensafely_job_name,
                                                                          "https://github.com/opensafely-core/test-public-repository.git", '',
                                                                          "c1ef0e676ec448b0a49e0073db364f36f6d6d078", "", allow_network_access, execute_job_image,
                                                                          execute_job_command, execute_job_arg, execute_job_env, {})
    
    assert set(jobs1) != set(jobs2)
    assert ws_pv_1 == ws_pv_2
    assert ws_pvc_1 == ws_pvc_2
    assert job_pv_1 != job_pv_2
    assert job_pvc_1 != job_pvc_2
    
    for job_name_1 in jobs1:
        status = await_job_status(job_name_1, namespace)
        log_k8s_job(job_name_1, namespace)
        assert status == K8SJobStatus.SUCCEEDED
    
    for job_name_2 in jobs2:
        status = await_job_status(job_name_2, namespace)
        log_k8s_job(job_name_2, namespace)
        assert status == K8SJobStatus.SUCCEEDED
    
    # clean up
    delete_namespace(namespace)
    
    delete_persistent_volume(ws_pv_1)
    delete_persistent_volume(job_pv_1)
    delete_persistent_volume(ws_pv_2)
    delete_persistent_volume(job_pv_2)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
@pytest.mark.skipif(not os.path.exists(K8S_TEST_CONFIG_FILE), reason="no k8s config found")
def test_create_duplicated_job(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner-tools:latest")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_WS_STORAGE_SIZE", "100M")
    monkeypatch.setattr("jobrunner.executors.graphnet.config.GRAPHNET_K8S_JOB_STORAGE_SIZE", "100M")
    
    init_k8s_config(True)
    
    allow_network_access = True
    execute_job_image = 'busybox'
    execute_job_command = ['/bin/sh', '-c']
    execute_job_arg = [f"echo job; ls -R -a /workspace;"]
    execute_job_env = {'OPENSAFELY_BACKEND': 'graphnet', 'DATABASE_URL': 'mssql://dummy_user:dummy_password@dummy_server:1433/dummy_db'}
    
    # same workspace, same name, same id
    workspace = "test_workspace"
    opensafely_job_name = "test_job_name"
    opensafely_job_id = "test_job_id"
    jobs1, ws_pv_1, ws_pvc_1, job_pv_1, job_pvc_1 = create_opensafely_job(workspace, opensafely_job_id, opensafely_job_name,
                                                                          "https://github.com/opensafely-core/test-public-repository.git", '',
                                                                          "c1ef0e676ec448b0a49e0073db364f36f6d6d078", "", allow_network_access, execute_job_image,
                                                                          execute_job_command, execute_job_arg, execute_job_env, {})
    
    # should not return error
    jobs2, ws_pv_2, ws_pvc_2, job_pv_2, job_pvc_2 = create_opensafely_job(workspace, opensafely_job_id, opensafely_job_name,
                                                                          "https://github.com/opensafely-core/test-public-repository.git", '',
                                                                          "c1ef0e676ec448b0a49e0073db364f36f6d6d078", "", allow_network_access, execute_job_image,
                                                                          execute_job_command, execute_job_arg, execute_job_env, {})
    
    for job_name_1 in jobs1:
        status = await_job_status(job_name_1, namespace)
        assert status == K8SJobStatus.SUCCEEDED
        log_k8s_job(job_name_1, namespace)
    
    for job_name_2 in jobs2:
        status = await_job_status(job_name_2, namespace)
        log_k8s_job(job_name_2, namespace)
        assert status == K8SJobStatus.SUCCEEDED
    
    # clean up
    delete_namespace(namespace)
    delete_persistent_volume(ws_pv_1)
    delete_persistent_volume(job_pv_1)
    delete_persistent_volume(ws_pv_2)
    delete_persistent_volume(job_pv_2)


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
    logs = read_log(job_name, namespace)
    for (pod_name, c), log in logs.items():
        print(f"--Log {pod_name}/{c} start:")
        print(log)
        print(f"--Log {pod_name}/{c} end\n")
    
    print("-" * 10, "end of log", job_name, "-" * 10, "\n")


# TODO Deprecated - replaced by JobAPI
def create_opensafely_job(workspace_name, opensafely_job_id, opensafely_job_name, repo_url, private_repo_access_token, commit_sha, inputs, allow_network_access,
                          execute_job_image, execute_job_command, execute_job_arg, execute_job_env, output_spec):
    from jobrunner.executors.graphnet.k8s_runner import graphnet_config, prepare, execute, read_k8s_job_status, finalize
    
    """
    1. create pv and pvc (ws_pvc) for the workspace if not exist
    2. check if the job exists, skip the job if already created
    3. create pv and pvc (job_pvc) for the job
    4. create a k8s job with ws_pvc and job_pvc mounted, this job consists of multiple steps running in multiple containers:
       1. pre container: git checkout study repo to job volume
       2. job container: run the opensafely job command (e.g. cohortextractor) on job_volume
       3. post container: use python re to move matching output files from job volume to ws volume
    """
    work_pv = convert_k8s_name(workspace_name, "pv")
    work_pvc = convert_k8s_name(workspace_name, "pvc")
    job_pv = convert_k8s_name(opensafely_job_id, "pv")
    job_pvc = convert_k8s_name(opensafely_job_id, "pvc")
    
    storage_class = graphnet_config.GRAPHNET_K8S_STORAGE_CLASS
    ws_pv_size = graphnet_config.GRAPHNET_K8S_WS_STORAGE_SIZE
    job_pv_size = graphnet_config.GRAPHNET_K8S_JOB_STORAGE_SIZE
    host_path = {"path": f"/tmp/{str(int(time.time() * 10 ** 6))}"} if graphnet_config.GRAPHNET_K8S_USE_LOCAL_CONFIG else None
    create_pv(work_pv, storage_class, ws_pv_size, host_path)
    create_pv(job_pv, storage_class, job_pv_size, host_path)
    
    namespace = graphnet_config.GRAPHNET_K8S_NAMESPACE
    create_namespace(namespace)
    
    create_pvc(work_pv, work_pvc, storage_class, namespace, ws_pv_size)
    create_pvc(job_pv, job_pvc, storage_class, namespace, job_pv_size)
    
    # Prepare
    prepare_job_name = convert_k8s_name(opensafely_job_name, "prepare", additional_hash=opensafely_job_id)
    prepare_job_name = prepare(prepare_job_name, commit_sha, inputs, job_pvc, private_repo_access_token, repo_url, work_pvc)
    
    # wait until prepare succeed
    await_job_status(prepare_job_name, namespace)
    
    # Execute
    whitelist = graphnet_config.GRAPHNET_K8S_EXECUTION_HOST_WHITELIST
    whitelist_network_labels = create_network_policy(namespace, [ip_port.split(":") for ip_port in whitelist.split(",")] if len(whitelist.strip()) > 0 else [])
    deny_all_network_labels = create_network_policy(namespace, [])
    execute_job_name = convert_k8s_name(opensafely_job_name, "execute", additional_hash=opensafely_job_id)
    network_labels = whitelist_network_labels if allow_network_access else deny_all_network_labels
    execute_job_name = execute(execute_job_name, execute_job_arg, execute_job_command, execute_job_env, execute_job_image, job_pvc, network_labels)
    
    # Finalize
    # wait for execute job finished before
    await_job_status(execute_job_name, namespace)
    
    finalize_job_name = convert_k8s_name(opensafely_job_name, "finalize", additional_hash=opensafely_job_id)
    finalize_job_name = finalize(finalize_job_name, execute_job_arg[0], execute_job_name, job_pvc, output_spec, work_pvc, workspace_name)
    
    return [prepare_job_name, execute_job_name, finalize_job_name], work_pv, work_pvc, job_pv, job_pvc
