#!/bin/bash

set -a
source .env.local
set +a

# run separately:
python -m jobrunner.sync
python -m jobrunner.run

#eval $(minikube docker-env)
# docker build -t opensafely-job-runner:latest .
# docker build -t cohortextractor:latest -f ../cohort-extractor/Dockerfile ../cohort-extractor/.
# docker tag cohortextractor:latest ghcr.io/opensafely-core/cohortextractor:latest
