import hashlib
import json
import re
import socket
import time
from enum import Enum
from typing import Tuple, Optional, List, Mapping

from kubernetes import client, config as k8s_config

from jobrunner.executors.graphnet.container.finalize import JOB_RESULTS_TAG

JOB_CONTAINER_NAME = "job"

batch_v1: client.BatchV1Api
core_v1: client.CoreV1Api
networking_v1: client.NetworkingV1Api


def init_k8s_config(local):
    global batch_v1, core_v1, networking_v1
    if local:
        # for testing, e.g. run on a local minikube
        k8s_config.load_kube_config()
    else:
        # Use the ServiceAccount in the cluster
        k8s_config.load_incluster_config()
    
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()
    networking_v1 = client.NetworkingV1Api()
    
    return batch_v1, core_v1, networking_v1


class K8SJobStatus(Enum):
    SUCCEEDED = 0  # 0 for success
    UNKNOWN = 1
    PENDING = 2
    RUNNING = 3
    
    FAILED = 4
    
    def completed(self):
        return self == K8SJobStatus.SUCCEEDED or self == K8SJobStatus.FAILED


def convert_k8s_name(text: str, suffix: Optional[str] = None, hash_len: int = 7, additional_hash: str = None) -> str:
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
    
    data = text
    if additional_hash is not None:
        data += additional_hash
    sha1 = hashlib.sha1(data.encode()).hexdigest()[:hash_len]
    clean_text = f"{clean_text[:max_len - hash_len - 1]}-{sha1}"
    
    if suffix is not None:
        clean_text += f"-{suffix}"
    
    return clean_text


def create_namespace(name: str):
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
    print(f"namespace {name} created")


def create_pv(pv_name: str, storage_class: str, size: str, host_path=None):
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
                    host_path=host_path
            )
    )
    core_v1.create_persistent_volume(body=pv)
    print(f"pv {pv_name} created")


def is_pvc_created(pvc_name: str) -> bool:
    all_pvc = core_v1.list_persistent_volume_claim_for_all_namespaces()
    for pvc in all_pvc.items:
        if pvc.metadata.name == pvc_name:
            return True
    return False


def create_pvc(pv_name: str, pvc_name: str, storage_class: str, namespace: str, size: str):
    if is_pvc_created(pvc_name):
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


def create_k8s_job(
        job_name: str,
        namespace: str,
        image: str,
        command: List[str],
        args: List[str],
        env: Mapping[str, str],
        storages: List[Tuple[str, str, bool]],
        pod_labels: Mapping[str, str], depends_on: str = None,
        image_pull_policy: str = "IfNotPresent",
        block_until_created=True
):
    """
    Create k8s job dynamically. Do nothing if job with the same job_name already exist.

    @param job_name: unique identifier of the job
    @param namespace: k8s namespace
    @param image: docker image tag
    @param command: cmd for the job container
    @param args: args for the job container
    @param env: env for the job container
    @param storages: List of (pvc_name, volume_mount_path, is_control). The first storage with is_control equals True will be used for dependency control
                     if depends_on is specified
    @param pod_labels: k8s labels to be added into the pod. Can be used for other controls like network policy
    @param depends_on: k8s job_name of another job. This job will wait until the specified job finished before it starts.
    @param image_pull_policy: image_pull_policy of the container
    @param block_until_created: block this function until the job is created on the k8s cluster
    """
    
    all_jobs = batch_v1.list_namespaced_job(namespace)
    for job in all_jobs.items:
        if job.metadata.name == job_name:
            print(f"job {job_name} already exist")
            return
    
    volumes = []
    control_volume_mount = None
    job_volume_mounts = []
    for pvc, path, is_control in storages:
        volume_name = convert_k8s_name(pvc, 'vol')
        volume = client.V1Volume(
                name=volume_name,
                persistent_volume_claim={"claimName": pvc},
        )
        volume_mount = client.V1VolumeMount(
                mount_path=path,
                name=volume_name
        )
        
        volumes.append(volume)
        job_volume_mounts.append(volume_mount)
        if is_control:
            control_volume_mount = volume_mount
    
    # convert env
    k8s_env = [client.V1EnvVar(str(k), str(v)) for (k, v) in env.items()]
    
    job_container = client.V1Container(
            name=JOB_CONTAINER_NAME,
            image=image,
            image_pull_policy=image_pull_policy,
            command=command,
            args=args,
            env=k8s_env,
            volume_mounts=job_volume_mounts
    )
    
    if control_volume_mount:
        pre_container = client.V1Container(
                name="pre",
                image="busybox",
                image_pull_policy=image_pull_policy,
                command=['/bin/sh', '-c'],
                args=[f"while [ ! -f /{control_volume_mount.mount_path}/.control-{depends_on} ]; do sleep 1; done"],
                volume_mounts=[control_volume_mount],
        )
        post_container = client.V1Container(
                name="post",
                image="busybox",
                image_pull_policy=image_pull_policy,
                command=['/bin/sh', '-c'],
                args=[f"touch /{control_volume_mount.mount_path}/.control-{job_name}"],
                volume_mounts=[control_volume_mount]
        )
        
        if depends_on is None:
            init_containers = [job_container]
        else:
            init_containers = [pre_container, job_container]
        containers = [post_container]
    else:
        if depends_on is not None:
            raise Exception("There must be a control storage if depends_on is not None")
        else:
            init_containers = None
            containers = [job_container]
    
    job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                    name=job_name,
                    labels={
                        "app": "os-test"
                    }
            ),
            spec=client.V1JobSpec(
                    backoff_limit=0,
                    template=client.V1PodTemplateSpec(
                            metadata=client.V1ObjectMeta(
                                    name=job_name,
                                    labels=pod_labels
                            ),
                            spec=client.V1PodSpec(
                                    restart_policy="Never",
                                    volumes=volumes,
                                    init_containers=init_containers,
                                    containers=containers,
                            )
                    )
            )
    )
    batch_v1.create_namespaced_job(body=job, namespace=namespace)
    
    if block_until_created:
        while read_k8s_job_status(job_name, namespace) == K8SJobStatus.UNKNOWN:
            time.sleep(.5)
    
    print(f"job {job_name} created")


def create_network_policy(namespace, address_ports):
    if address_ports and len(address_ports) > 0:
        np_name = convert_k8s_name(f"allow-{'-'.join([f'{ip}:{port}' for ip, port in address_ports])}")
    else:
        np_name = convert_k8s_name(f"deny-all")
    pod_label = {
        'network': np_name
    }
    
    all_np = networking_v1.list_namespaced_network_policy(namespace)
    for np in all_np.items:
        if np.metadata.name == np_name:
            print(f"network policy {np_name} already exist")
            return
    
    # resolve ip for domain
    ip_ports = []
    for address, port in address_ports:
        ip_list = list({addr[-1][0] for addr in socket.getaddrinfo(address, 0, 0, 0, 0)})
        print(f'resolved ip for {address}: {ip_list}')
        for ip in ip_list:
            ip_ports.append([ip, port])
    
    # create egress whitelist
    egress = []
    for ip, port in ip_ports:
        rule = client.V1NetworkPolicyEgressRule(
                to=[
                    {
                        'ipBlock': {
                            'cidr': f'{ip}/32'
                        }
                    }
                ],
                ports=[
                    client.V1NetworkPolicyPort(
                            protocol='TCP',
                            port=int(port)
                    ),
                    client.V1NetworkPolicyPort(
                            protocol='UDP',
                            port=int(port)
                    ),
                ])
        egress.append(rule)
    
    network_policy = client.V1NetworkPolicy(
            metadata=client.V1ObjectMeta(
                    name=np_name
            ),
            spec=client.V1NetworkPolicySpec(
                    pod_selector=client.V1LabelSelector(
                            match_labels=pod_label
                    ),
                    policy_types=[
                        'Egress'
                    ],
                    egress=egress,
            )
    )
    
    networking_v1.create_namespaced_network_policy(namespace, network_policy)
    print(f"network policy {np_name} created for {'-'.join([f'{ip}:{port}' for ip, port in ip_ports])}")
    return pod_label


def read_k8s_job_status(job_name: str, namespace: str) -> K8SJobStatus:
    all_jobs = batch_v1.list_namespaced_job(namespace)
    job_found = False
    for job in all_jobs.items:
        if job.metadata.name == job_name:
            job_found = True
    if not job_found:
        return K8SJobStatus.UNKNOWN
    
    status = batch_v1.read_namespaced_job(f"{job_name}", namespace=namespace).status
    if status.succeeded:
        return K8SJobStatus.SUCCEEDED
    elif status.failed:
        return K8SJobStatus.FAILED
    elif status.active != 1:
        return K8SJobStatus.PENDING
    
    # Active
    pods = core_v1.list_namespaced_pod(namespace=namespace)
    job_pods_status = [p.status for p in pods.items if p.metadata.labels.get('job-name') == job_name]  # get must be used to avoid error when key not found
    
    init_container_statuses = job_pods_status[-1].init_container_statuses
    if init_container_statuses and len(init_container_statuses) > 0:
        waiting = init_container_statuses[-1].state.waiting
        if waiting and waiting.reason == 'ImagePullBackOff':
            return K8SJobStatus.FAILED  # Fail to pull the image in the init_containers
    
    container_statuses = job_pods_status[-1].container_statuses
    if container_statuses and len(container_statuses) > 0:
        waiting = container_statuses[-1].state.waiting
        if waiting and waiting.reason == 'ImagePullBackOff':
            return K8SJobStatus.FAILED  # Fail to pull the image in the containers
    
    return K8SJobStatus.RUNNING


def delete_job(job_name: str, namespace: str):
    pods = list_pod_of_job(job_name, namespace)
    
    for pod in pods:
        pod_name = pod.metadata.name
        core_v1.delete_namespaced_pod(pod_name, namespace)
    
    batch_v1.delete_namespaced_job(job_name, namespace)


def list_pod_of_job(job_name: str, namespace: str) -> List:
    # logs: read logs of the job
    pods = core_v1.list_namespaced_pod(namespace=namespace)
    pods = [p for p in pods.items if p.metadata.labels.get('job-name') == job_name]  # get must be used to avoid error when key not found
    return pods


def read_log(job_name: str, namespace: str) -> Mapping[Tuple[str, str], str]:
    # logs: read logs of the job
    pods = list_pod_of_job(job_name, namespace)
    
    logs = {}
    for pod in pods:
        pod_name = pod.metadata.name
        
        all_containers = []
        if pod.spec.init_containers:
            for container in pod.spec.init_containers:
                all_containers.append(container.name)
        for container in pod.spec.containers:
            all_containers.append(container.name)
        
        for container_name in all_containers:
            try:
                logs[(pod_name, container_name)] = core_v1.read_namespaced_pod_log(pod_name, namespace=namespace, container=container_name)
            except Exception as e:
                print(e)
    
    return logs


def extract_k8s_api_values(data, removed_fields):
    if isinstance(data, list):
        if len(data) == 0:
            return None
        else:
            return [extract_k8s_api_values(d, removed_fields) for d in data]
    elif isinstance(data, dict):
        if len(data) == 0:
            return None
        else:
            result = {}
            for key, value in data.items():
                if key in removed_fields:
                    result[key] = '<removed>'
                else:
                    extracted = extract_k8s_api_values(value, removed_fields)
                    if extracted is not None:
                        result[key] = extracted
            if len(result) == 0:
                return None
            else:
                return result
    elif hasattr(data, 'attribute_map'):
        result = {}
        attrs = data.attribute_map.keys()
        for key in attrs:
            if key in removed_fields:
                result[key] = '<removed>'
            else:
                value = getattr(data, key)
                if value is not None:
                    extracted = extract_k8s_api_values(value, removed_fields)
                    if extracted is not None:
                        result[key] = extracted
        if len(result) == 0:
            return None
        else:
            return result
    elif data is None:
        return None
    else:
        return str(data)


def read_finalize_output(opensafely_job_name, opensafely_job_id, namespace):
    finalize_job_name = convert_k8s_name(opensafely_job_name, "finalize", additional_hash=opensafely_job_id)
    logs = read_log(finalize_job_name, namespace)
    container_log = ""
    for (_, container_name), container_log in logs.items():
        if container_name == JOB_CONTAINER_NAME:
            break
    for line in container_log.split('\n'):
        if line.startswith(JOB_RESULTS_TAG):
            job_result = line[len(JOB_RESULTS_TAG):]
            return json.loads(job_result)
    return None


def read_image_id(job_name, container_name, namespace):
    pods = list_pod_of_job(job_name, namespace)
    for pod in pods:
        pod_name = pod.metadata.name
        pod_status = core_v1.read_namespaced_pod_status(pod_name, namespace)
        
        for container_status in pod_status.status.container_statuses:
            if container_status.name == container_name:
                image_id = container_status.image_id
                if image_id and len(image_id) > 0:  # may not be the final pod
                    return image_id
        
        if pod_status.status.init_container_statuses:
            for container_status in pod_status.status.init_container_statuses:
                if container_status.name == container_name:
                    image_id = container_status.image_id
                    if image_id and len(image_id) > 0:  # may not be the final pod
                        return image_id
    
    return None


def read_container_exit_code(job_name, container_name, namespace):
    pods = list_pod_of_job(job_name, namespace)
    for pod in pods:
        pod_name = pod.metadata.name
        pod_status = core_v1.read_namespaced_pod_status(pod_name, namespace)
        
        for container_status in pod_status.status.container_statuses:
            if container_status.name == container_name:
                exit_code = container_status.state.terminated.exit_code
                if exit_code is not None:  # may not be the final pod
                    return exit_code
        
        if pod_status.status.init_container_statuses:
            for container_status in pod_status.status.init_container_statuses:
                if container_status.name == container_name:
                    exit_code = container_status.state.terminated.exit_code
                    if exit_code is not None:  # may not be the final pod
                        return exit_code
    
    return None


def await_job_status(job_name, namespace, sleep_interval=.5, timeout=5 * 60 * 60) -> Optional[K8SJobStatus]:
    # describe: read the status of the job until succeeded or failed
    start_time = time.time()
    while time.time() - start_time < timeout:
        status = read_k8s_job_status(job_name, namespace)
        if status.completed():
            print("job completed")
            return status
        time.sleep(sleep_interval)
    return None
