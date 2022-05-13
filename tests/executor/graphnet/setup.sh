minikube config set memory 2048
# https://docs.projectcalico.org/getting-started/kubernetes/minikube
minikube start --network-plugin=cni --cni=calico --mount-string="/tmp/minikube-data:/tmp"

kubectl config get-contexts
kubectl config use-context minikube

# check that cilium pod is ok:
watch kubectl get pods -l k8s-app=calico-node -A

eval $(minikube docker-env)

export DOCKER_BUILDKIT=1

# job runner
docker build --target job-runner-graphnet -t ccbidevdsacr.azurecr.io/opensafely-job-runner-graphnet:latest -f ../../../docker/Dockerfile ../../../.
docker build --target job-runner-tools-graphnet -t ccbidevdsacr.azurecr.io/opensafely-job-runner-tools:latest -f ../../../docker/Dockerfile ../../../.

# cohort extractor
docker build -t ccbidevdsacr.azurecr.io/cohortextractor:latest -f ../../../../cohort-extractor/Dockerfile ../../../../cohort-extractor/.

# other images
docker pull busybox:latest
docker pull curlimages/curl:latest
