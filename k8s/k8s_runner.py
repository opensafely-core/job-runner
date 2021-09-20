from __future__ import print_function, unicode_literals, division, absolute_import

import hashlib
import pickle
import re
import time

from jobrunner import config

from kubernetes import client
from kubernetes import config as k8s_config

batch_v1 = client.BatchV1Api()
core_v1 = client.CoreV1Api()


def init_k8s_config():
    global batch_v1, core_v1
    if config.K8S_USE_LOCAL_CONFIG:
        # for testing, e.g. run on a local minikube
        k8s_config.load_kube_config()
    else:
        # Use the ServiceAccount in the cluster
        k8s_config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()


def create_opensafely_job(workspace_name, opensafely_job_id, opensafely_job_name, repo_url, commit_sha, inputs):
    """
    1. create pv and pvc (ws_pvc) for the workspace if not exist
    2. check if the job exists, skip the job if already created
    3. create pv and pvc (job_pvc) for the job
    4. create a k8s job with ws_pvc and job_pvc mounted, this job consists of multiple steps running in multiple containers:
       1. pre container: git checkout study repo to job volume
       2. job container: run the opensafely job command (e.g. cohortextractor) on job_volume
       3. post container: use python re to move matching output files from job volume to ws volume
    """
    size = '100M' if config.K8S_USE_LOCAL_CONFIG else '20Gi'
    
    ws_pv = convert_k8s_name(workspace_name, "pv")
    job_pv = convert_k8s_name(opensafely_job_id, "pv")
    create_pv(ws_pv, config.K8S_STORAGE_CLASS, size)
    create_pv(job_pv, config.K8S_STORAGE_CLASS, size)
    
    create_namespace(config.K8S_NAMESPACE)
    
    ws_pvc = convert_k8s_name(workspace_name, "pvc")
    create_pvc(ws_pv, ws_pvc, config.K8S_STORAGE_CLASS, config.K8S_NAMESPACE, size)
    
    job_pvc = convert_k8s_name(opensafely_job_id, "pvc")
    create_pvc(job_pv, job_pvc, config.K8S_STORAGE_CLASS, config.K8S_NAMESPACE, size)
    
    job_name = convert_k8s_name(opensafely_job_name, "job")
    namespace = config.K8S_NAMESPACE
    jobrunner_image = config.K8S_JOB_RUNNER_IMAGE
    ws_dir = "/ws_volume"
    job_dir = "/job_volume"
    
    create_job_with_pvc(job_name, ws_pvc, job_pvc, namespace, jobrunner_image, repo_url, commit_sha, ws_dir, job_dir, inputs)
    
    return job_name, ws_pv, ws_pvc, job_pv, job_pvc


def convert_k8s_name(text, suffix=None, hash_len=7):
    """
    convert the text to the name follow the standard:
    https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names
    """
    NAME_MAX_LEN = 63  # max len of name based on the spec
    
    # remove all invalid chars
    def remove_invalid_char(t):
        if t is None:
            return None
        
        t = t.lower()
        t = re.sub(r'[^a-z0-9-]+', "-", t)
        t = re.sub(r'-+', "-", t)
        t = re.sub(r'^[^a-z]+', "", t)
        t = re.sub(r'[^a-z0-9]+$', "", t)
        return t
    
    clean_text = remove_invalid_char(text)
    suffix = remove_invalid_char(suffix)
    
    # limit the length
    max_len = NAME_MAX_LEN
    if suffix is not None:
        max_len -= len(suffix) + 1
    
    sha1 = hashlib.sha1(text.encode()).hexdigest()[:hash_len]
    clean_text = f"{clean_text[:max_len - hash_len - 1]}-{sha1}"
    
    if suffix is not None:
        clean_text += f"-{suffix}"
    
    return clean_text


def create_namespace(name):
    if name == 'default':
        print(f"default namespace is used")
        return
    
    namespaces = core_v1.list_namespace()
    if name in [n.metadata.name for n in namespaces.items]:
        print(f"namespace {name} already exist")
        return
    
    core_v1.create_namespace(client.V1Namespace(
            metadata=client.V1ObjectMeta(
                    name=name
            )
    ))


def create_pv(pv_name, storage_class, size):
    all_pv = core_v1.list_persistent_volume()
    for pv in all_pv.items:
        if pv.metadata.name == pv_name:
            print(f"pv {pv_name} already exist")
            return
    
    pv = client.V1PersistentVolume(
            metadata=client.V1ObjectMeta(
                    name=pv_name,
                    labels={
                        "app": "opensafely"
                    }
            ),
            spec=client.V1PersistentVolumeSpec(
                    storage_class_name=storage_class,
                    capacity={
                        "storage": size
                    },
                    access_modes=["ReadWriteOnce"],
                    
                    # for testing:
                    # host_path={"path": f"/pv/{pv_name}"} if config.K8S_USE_LOCAL_CONFIG else None
            )
    )
    core_v1.create_persistent_volume(body=pv)


def create_pvc(pv_name, pvc_name, storage_class, namespace, size):
    all_pvc = core_v1.list_persistent_volume_claim_for_all_namespaces()
    for pvc in all_pvc.items:
        if pvc.metadata.name == pvc_name:
            print(f"pvc {pvc_name} already exist")
            return
    
    pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(
                    name=pvc_name,
                    labels={
                        "app": "opensafely"
                    }
            ),
            spec=client.V1PersistentVolumeClaimSpec(
                    storage_class_name=storage_class,
                    volume_name=pv_name,
                    access_modes=["ReadWriteOnce"],
                    resources={
                        "requests": {
                            "storage": size
                        }
                    }
            )
    )
    core_v1.create_namespaced_persistent_volume_claim(body=pvc, namespace=namespace)
    print(f"pvc {pvc_name} created")


def create_job_with_pvc(job_name, ws_pvc, job_pvc, namespace, job_runner_image, repo_url, commit_sha, ws_dir, job_dir, inputs):
    ws_volume = "ws-volume"
    job_volume = "job-volume"
    
    repos_dir = ws_dir + "/repos"
    job_workspace = job_dir + "/workspace"
    
    job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                    name=job_name,
                    labels={
                        "app": "opensafely"
                    }
            ),
            spec=client.V1JobSpec(
                    template=client.V1PodTemplateSpec(
                            metadata=client.V1ObjectMeta(
                                    name=job_name,
                                    labels={
                                        "app": "opensafely"
                                    }
                            ),
                            spec=client.V1PodSpec(
                                    restart_policy="Never",
                                    volumes=[
                                        client.V1Volume(
                                                name=ws_volume,
                                                persistent_volume_claim={
                                                    "claimName": ws_pvc
                                                }),
                                        client.V1Volume(
                                                name=job_volume,
                                                persistent_volume_claim={
                                                    "claimName": job_pvc
                                                }),
                                    ],
                                    init_containers=[
                                        client.V1Container(
                                                name="pre",
                                                image=job_runner_image,
                                                image_pull_policy="Never" if config.K8S_USE_LOCAL_CONFIG else "Always",
                                                command=['python', '-m', 'k8s.pre'],
                                                args=[repo_url, commit_sha, repos_dir, job_workspace, inputs],
                                                volume_mounts=[
                                                    client.V1VolumeMount(
                                                            mount_path=ws_dir,
                                                            name=ws_volume
                                                    ),
                                                    client.V1VolumeMount(
                                                            mount_path=job_dir,
                                                            name=job_volume
                                                    ),
                                                ]
                                        ),
                                        # TODO
                                        client.V1Container(
                                                name="job",
                                                image="busybox",
                                                image_pull_policy="Never",
                                                command=['/bin/sh', '-c'],
                                                args=[f"echo ws; ls -R {ws_dir}; echo job; ls -R {job_dir};"],
                                                volume_mounts=[
                                                    client.V1VolumeMount(
                                                            mount_path=ws_dir,
                                                            name=ws_volume
                                                    ),
                                                    client.V1VolumeMount(
                                                            mount_path=job_dir,
                                                            name=job_volume
                                                    )
                                                ]
                                        )
                                    ],
                                    # TODO
                                    containers=[client.V1Container(
                                            name="post",
                                            image="busybox",
                                            image_pull_policy="Never",
                                            command=['/bin/sh', '-c'],
                                            args=["echo post"],
                                            volume_mounts=[
                                                client.V1VolumeMount(
                                                        mount_path=ws_dir,
                                                        name=ws_volume
                                                ),
                                                client.V1VolumeMount(
                                                        mount_path=job_dir,
                                                        name=job_volume
                                                ),
                                            ]
                                    )],
                            )
                    )
            )
    )
    batch_v1.create_namespaced_job(body=job, namespace=namespace)


def control_job(job_name, namespace):
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
    
    # logs:  read logs of the job
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
    
    # delete: delete job and pods
    batch_v1.delete_namespaced_job(job_name, namespace=namespace)
    for pod_name in job_pod_names:
        core_v1.delete_namespaced_pod(pod_name, namespace=namespace)
