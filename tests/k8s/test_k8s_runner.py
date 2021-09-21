import pickle
import re
import time

import pytest

from jobrunner.k8s.k8s_runner import (
    init_k8s_config,
    create_opensafely_job,
    convert_k8s_name,
)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
def test_create_opensafely_job(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.config.K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.config.K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.config.K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner:latest")
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_SIZE", "100M")
    
    init_k8s_config()
    
    job_name, ws_pv, _, job_pv, _ = create_opensafely_job("test_workspace", "test_job_id", "test_job_name",
                                                          "https://github.com/opensafely-core/test-public-repository.git",
                                                          "c1ef0e676ec448b0a49e0073db364f36f6d6d078", "")
    
    from kubernetes import client
    
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()
    
    assert_job_succeeded(batch_v1, job_name, namespace)
    
    log_k8s_job(core_v1, job_name, namespace)

    # clean up
    core_v1.delete_namespace(namespace)
    core_v1.delete_persistent_volume(ws_pv)
    core_v1.delete_persistent_volume(job_pv)


@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
def test_create_opensafely_job_concurrent(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.config.K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.config.K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.config.K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner:latest")
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_SIZE", "100M")
    
    init_k8s_config()

    # same workspace, same name, but different id
    workspace = "test_workspace"
    opensafely_job_name = "test_job_name"
    job_name_1, ws_pv_1, ws_pvc_1, job_pv_1, job_pvc_1 = create_opensafely_job(workspace, "test_job_id_1", opensafely_job_name,
                                                                               "https://github.com/opensafely-core/test-public-repository.git",
                                                                               "c1ef0e676ec448b0a49e0073db364f36f6d6d078", "")
    
    job_name_2, ws_pv_2, ws_pvc_2, job_pv_2, job_pvc_2 = create_opensafely_job(workspace, "test_job_id_2", opensafely_job_name,
                                                                               "https://github.com/opensafely-core/test-public-repository.git",
                                                                               "c1ef0e676ec448b0a49e0073db364f36f6d6d078", "")
    
    assert job_name_1 != job_name_2
    assert ws_pv_1 == ws_pv_2
    assert ws_pvc_1 == ws_pvc_2
    assert job_pv_1 != job_pv_2
    assert job_pvc_1 != job_pvc_2
    
    from kubernetes import client
    
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()
    
    assert_job_succeeded(batch_v1, job_name_1, namespace)
    assert_job_succeeded(batch_v1, job_name_2, namespace)
    
    log_k8s_job(core_v1, job_name_1, namespace)
    log_k8s_job(core_v1, job_name_2, namespace)
    
    # clean up
    core_v1.delete_namespace(namespace)
    core_v1.delete_persistent_volume(ws_pv_1)
    core_v1.delete_persistent_volume(job_pv_1)
    core_v1.delete_persistent_volume(ws_pv_2)
    core_v1.delete_persistent_volume(job_pv_2)

@pytest.mark.slow_test
@pytest.mark.needs_local_k8s
def test_create_opensafely_job_duplicated(monkeypatch):
    namespace = "opensafely-test"
    
    monkeypatch.setattr("jobrunner.config.K8S_USE_LOCAL_CONFIG", 1)
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_CLASS", "standard")
    monkeypatch.setattr("jobrunner.config.K8S_NAMESPACE", namespace)
    monkeypatch.setattr("jobrunner.config.K8S_JOB_RUNNER_IMAGE", "opensafely-job-runner:latest")
    monkeypatch.setattr("jobrunner.config.K8S_STORAGE_SIZE", "100M")
    
    init_k8s_config()

    # same workspace, same name, same id
    workspace = "test_workspace"
    opensafely_job_name = "test_job_name"
    opensafely_job_id = "test_job_id"
    job_name_1, ws_pv_1, ws_pvc_1, job_pv_1, job_pvc_1 = create_opensafely_job(workspace, opensafely_job_id, opensafely_job_name,
                                                                               "https://github.com/opensafely-core/test-public-repository.git",
                                                                               "c1ef0e676ec448b0a49e0073db364f36f6d6d078", "")
    
    # should not return error
    job_name_2, ws_pv_2, ws_pvc_2, job_pv_2, job_pvc_2 = create_opensafely_job(workspace, opensafely_job_id, opensafely_job_name,
                                                                               "https://github.com/opensafely-core/test-public-repository.git",
                                                                               "c1ef0e676ec448b0a49e0073db364f36f6d6d078", "")
    
    from kubernetes import client
    
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()
    
    assert_job_succeeded(batch_v1, job_name_1, namespace)
    assert_job_succeeded(batch_v1, job_name_2, namespace)
    
    log_k8s_job(core_v1, job_name_1, namespace)
    log_k8s_job(core_v1, job_name_2, namespace)
    
    # clean up
    core_v1.delete_namespace(namespace)
    core_v1.delete_persistent_volume(ws_pv_1)
    core_v1.delete_persistent_volume(job_pv_1)
    core_v1.delete_persistent_volume(ws_pv_2)
    core_v1.delete_persistent_volume(job_pv_2)


def assert_job_succeeded(batch_v1, job_name, namespace):
    # describe: read the status of the job until succeeded or failed
    last_status = None
    while True:
        status = batch_v1.read_namespaced_job(f"{job_name}", namespace=namespace).status
        if pickle.dumps(last_status) != pickle.dumps(status):
            print(status)
        last_status = status
        if status.succeeded or status.failed:
            print("job completed")
            break
        time.sleep(2)
    assert last_status.succeeded


def log_k8s_job(core_v1, job_name, namespace):
    # logs: read logs of the job
    pods = core_v1.list_namespaced_pod(namespace=namespace)
    job_pod_names = [p.metadata.name for p in pods.items if p.metadata.labels.get('job-name') == job_name]  # get must be used to avoid error when key not found
    for pod_name in job_pod_names:
        print("-" * 10, "start of log", job_name, "-" * 10)
        log = core_v1.read_namespaced_pod_log(pod_name, namespace=namespace, container="pre")
        print(log)
        log = core_v1.read_namespaced_pod_log(pod_name, namespace=namespace, container="job")
        print(log)
        log = core_v1.read_namespaced_pod_log(pod_name, namespace=namespace, container="post")
        print(log)
        print("-" * 10, "end of log", job_name, "-" * 10)


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
