import os

# k8s configurations:
# Set to 1 to access the k8s cluster locally as if using the `kubectl` command
GRAPHNET_K8S_USE_LOCAL_CONFIG = os.environ.get("GRAPHNET_K8S_USE_LOCAL_CONFIG", "0") == "1"

# Image pull policy, IfNotPresent if not defined
GRAPHNET_K8S_IMAGE_PULL_POLICY = os.environ.get("GRAPHNET_K8S_IMAGE_PULL_POLICY", "IfNotPresent")

# 1 if want to run the k8s executor in a local environment, e.g. minikube
GRAPHNET_K8S_USE_LOCAL_STORAGE = os.environ.get("GRAPHNET_K8S_USE_LOCAL_STORAGE", "0") == "1"

# Storage class to be used, platform dependent
GRAPHNET_K8S_STORAGE_CLASS = os.environ.get("GRAPHNET_K8S_STORAGE_CLASS", "standard")

# Namespace to be used
GRAPHNET_K8S_NAMESPACE = os.environ.get("GRAPHNET_K8S_NAMESPACE", "opensafely")

# Location of the image of the job runner tool, i.e. the prepare and finalize container
GRAPHNET_K8S_JOB_RUNNER_TOOL_IMAGE = os.environ.get("GRAPHNET_K8S_JOB_RUNNER_TOOL_IMAGE", "ghcr.io/opensafely-core/opensafely-job-runner-tools:latest")

# The size of the workspace storage
GRAPHNET_K8S_WS_STORAGE_SIZE = os.environ.get("GRAPHNET_K8S_WS_STORAGE_SIZE", "20Gi")

# The size of the job storage
GRAPHNET_K8S_JOB_STORAGE_SIZE = os.environ.get("GRAPHNET_K8S_JOB_STORAGE_SIZE", "20Gi")

# The comma separated list of IP:PORT to be whitelisted by the execution job when `allow_database_access` is True
GRAPHNET_K8S_EXECUTION_HOST_WHITELIST = os.environ.get("GRAPHNET_K8S_EXECUTION_HOST_WHITELIST", "")

# Keep the job in cleanup when the job is failed
GRAPHNET_K8S_KEEP_FAILED_JOB = os.environ.get("GRAPHNET_K8S_KEEP_FAILED_JOB", "0") == "1"

# the k8s service account to be used by the finalize container to access the logs of the job
GRAPHNET_K8S_JOB_SERVICE_ACCOUNT = os.environ.get("GRAPHNET_K8S_JOB_SERVICE_ACCOUNT", None)

# use a single pv as the workdir for all the workspace
GRAPHNET_K8S_USE_SINGLE_WORKDIR_STORAGE = os.environ.get("GRAPHNET_K8S_USE_SINGLE_WORKDIR_STORAGE", "0") == "1"
