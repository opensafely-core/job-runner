import os

# k8s configurations:
# 1 if want to run the k8s_runner in a local environment, e.g. minikube
GRAPHNET_K8S_USE_LOCAL_CONFIG = os.environ.get("GRAPHNET_K8S_USE_LOCAL_CONFIG", "0") == "1"

# Storage class to be used, platform dependent
GRAPHNET_K8S_STORAGE_CLASS = os.environ.get("GRAPHNET_K8S_STORAGE_CLASS", "standard")

# Namespace to be used
GRAPHNET_K8S_NAMESPACE = os.environ.get("GRAPHNET_K8S_NAMESPACE", "opensafely")

# Location of the image of the job runner
GRAPHNET_K8S_JOB_RUNNER_IMAGE = os.environ.get("GRAPHNET_K8S_JOB_RUNNER_IMAGE", "ghcr.io/opensafely-core/job-runner:latest")

# The size of the workspace storage
GRAPHNET_K8S_WS_STORAGE_SIZE = os.environ.get("GRAPHNET_K8S_WS_STORAGE_SIZE", "20Gi")

# The size of the workspace storage
GRAPHNET_K8S_JOB_STORAGE_SIZE = os.environ.get("GRAPHNET_K8S_JOB_STORAGE_SIZE", "20Gi")

# The comma separated list of IP:PORT to be whitelisted by the execution job when `allow_database_access` is True
GRAPHNET_K8S_EXECUTION_HOST_WHITELIST = os.environ.get("GRAPHNET_K8S_EXECUTION_HOST_WHITELIST", "")
