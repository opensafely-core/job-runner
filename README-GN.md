# Documentation of using the job-server code in Graphnet

# Get started

The `job-runner.sync` is a program to synchronise job between job-server and local sqlite db
The `job-service.run` is a program to execute jobs in the local sqlite db using docker


# modification

Change the following code in the project

1. Modified docker registry locat method to support local images
2. Added debug functions to allow printing command executed, e.g. docker run ...
3. Added debug model to run with dummy data

# local test

The program can be started locally with the [local-test.sh](./local-test.sh). This file depends on a private `.env.graphnet` file, which is not included int this repository for security reason. But these are the environment variables located in this file:

- JOB_SERVER_ENDPOINT - the location of job server, e.g. 'http://host.docker.internal:8000/api/v2/'
- JOB_SERVER_TOKEN - the GRAPHNET_BACKEND_TOKEN in job-server
- BACKEND - graphnet
- DEBUG - set it to 1 to turn on the command log and use dummy data
- FULL_DATABASE_URL
- SLICE_DATABASE_URL
- DUMMY_DATABASE_URL
- HIGH_PRIVACY_HOST_DIR - the host dir for /workdir/high_privacy
- MEDIUM_PRIVACY_HOST_DIR - the host dir for /workdir/medium_privacy
- HIGH_PRIVACY_STORAGE_BASE - the container dir for /app/workdir/high_privacy
- MEDIUM_PRIVACY_STORAGE_BASE - the container dir for /app/workdir/medium_privacy
- WORK_DIR - the container dir for /workdir
- DOCKER_REGISTRY - 'ghcr.io/opensafely-core/' or 'ccbidevdsacr.azurecr.io'
- PRIVATE_REPO_ACCESS_TOKEN - GitHub -> setting -> Personal access tokens
