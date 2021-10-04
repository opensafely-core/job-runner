import pickle
import re
import time
import random
import pytest

from jobrunner.k8s.k8s_runner import (
    init_k8s_config,
    create_opensafely_job,
    convert_k8s_name,
    create_k8s_job,
    read_log,
    create_pv,
    create_pvc,
    create_namespace,
    create_network_policy,
    read_k8s_job_status,
    JobStatus,
    read_job_status
)


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


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
def test_job_env(monkeypatch):
    monkeypatch.setattr("jobrunner.config.K8S_USE_LOCAL_CONFIG", 1)
    
    namespace = "opensafely-test"
    
    init_k8s_config()
    
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
    assert status == JobStatus.SUCCEEDED
    log = list(logs.values())[0]
    for k, v in env.items():
        assert f"{k}={v}" in log
    
    # clean up
    delete_namespace(namespace)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
def test_job_network(monkeypatch):
    monkeypatch.setattr("jobrunner.config.K8S_USE_LOCAL_CONFIG", 1)
    
    init_k8s_config()
    
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
    
    assert status1 == JobStatus.SUCCEEDED
    assert status2 == JobStatus.FAILED
    
    # clean up
    delete_namespace(namespace)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
def test_job_sequence(monkeypatch):
    monkeypatch.setattr("jobrunner.config.K8S_USE_LOCAL_CONFIG", 1)
    
    namespace = "opensafely-test"
    pv_name = "job-pv"
    pvc_name = "job-pvc"
    storage_class = "standard"
    size = "100M"
    
    init_k8s_config()
    
    create_namespace(namespace)
    create_pv(pv_name, storage_class, size)
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
        assert status == JobStatus.SUCCEEDED
        
        status = batch_v1.read_namespaced_job(job_name, namespace=namespace).status
        if last_completion_time:
            assert last_completion_time < status.completion_time
        last_completion_time = status.completion_time
    
    # clean up
    delete_namespace(namespace)
    delete_persistent_volume(pv_name)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
def test_generate_cohort(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.config.K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.config.K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.config.K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner:latest")
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_SIZE", "100M")
    monkeypatch.setattr("jobrunner.config.K8S_EXECUTION_HOST_WHITELIST", "127.0.0.1:1433")
    
    init_k8s_config()
    
    import configparser
    config = configparser.RawConfigParser()
    config.read('private_test_config.ini')
    
    workspace_name = "test_workspace"
    opensafely_job_id = "test_job_id"
    opensafely_job_name = "test_job_name"
    repo_url = "https://github.com/graphnet-opensafely/opensafely-SRO-Measures.git"
    commit_sha = "8cfdfbaadbc63c7b5023609731f4a591e3e279fa"
    private_repo_access_token = config.get('git', 'PRIVATE_REPO_ACCESS_TOKEN')
    inputs = ""
    output_spec = {'highly_sensitive': {'cohort': 'output/input_*.csv'}}
    
    allow_network_access = True
    execute_job_image = 'ghcr.io/opensafely-core/cohortextractor:latest'
    execute_job_command = None
    execute_job_arg = ['generate_cohort', '--study-definition', 'study_definition', '--index-date-range', '2021-01-01 to 2021-02-01 by month', '--output-dir=output',
                       '--output-dir=output', '--expectations-population=1']
    execute_job_env = {'OPENSAFELY_BACKEND': 'graphnet', 'DATABASE_URL': 'mssql://dummy_user:dummy_password@127.0.0.1:1433/dummy_db'}
    
    jobs, ws_pv, _, job_pv, _ = create_opensafely_job(workspace_name, opensafely_job_id, opensafely_job_name, repo_url, private_repo_access_token, commit_sha, inputs,
                                                      allow_network_access, execute_job_image, execute_job_command, execute_job_arg, execute_job_env, output_spec)
    
    for job_name in jobs:
        status = await_job_status(job_name, namespace)
        log_k8s_job(job_name, namespace)
        assert status == JobStatus.SUCCEEDED
    
    job_status = read_job_status(opensafely_job_name, opensafely_job_id, namespace)
    print(job_status)
    assert job_status == {'outputs': {'output/input_2021-01-01.csv': 'highly_sensitive', 'output/input_2021-02-01.csv': 'highly_sensitive'}, 'unmatched': []}
    
    # clean up
    delete_namespace(namespace)
    
    delete_persistent_volume(ws_pv)
    delete_persistent_volume(job_pv)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
def test_create_concurrent_jobs(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.config.K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.config.K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.config.K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner:latest")
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_SIZE", "100M")
    
    init_k8s_config()
    
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
        assert status == JobStatus.SUCCEEDED
    
    for job_name_2 in jobs2:
        status = await_job_status(job_name_2, namespace)
        log_k8s_job(job_name_2, namespace)
        assert status == JobStatus.SUCCEEDED
    
    # clean up
    delete_namespace(namespace)
    
    delete_persistent_volume(ws_pv_1)
    delete_persistent_volume(job_pv_1)
    delete_persistent_volume(ws_pv_2)
    delete_persistent_volume(job_pv_2)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
def test_create_duplicated_job(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.config.K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.config.K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.config.K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner:latest")
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_SIZE", "100M")
    
    init_k8s_config()
    
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
        assert status == JobStatus.SUCCEEDED
        log_k8s_job(job_name_1, namespace)
    
    for job_name_2 in jobs2:
        status = await_job_status(job_name_2, namespace)
        log_k8s_job(job_name_2, namespace)
        assert status == JobStatus.SUCCEEDED
    
    # clean up
    delete_namespace(namespace)
    delete_persistent_volume(ws_pv_1)
    delete_persistent_volume(job_pv_1)
    delete_persistent_volume(ws_pv_2)
    delete_persistent_volume(job_pv_2)


def await_job_status(job_name, namespace):
    # describe: read the status of the job until succeeded or failed
    while True:
        status = read_k8s_job_status(job_name, namespace)
        if status.completed():
            print("job completed")
            return status
        time.sleep(.5)


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
