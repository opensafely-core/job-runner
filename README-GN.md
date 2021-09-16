# Documentation of using the job-server code in Graphnet

# Get started

The `job-runner.sync` is a program to synchronise job between job-server and local sqlite db
The `job-service.run` is a program to execute jobs in the local sqlite db using docker


# Modifications

Change the following code in the project

1. Modified docker registry locat method to support local images
2. Added debug functions to allow printing command executed, e.g. docker run ...
3. Added debug model to run with dummy data

# Local test

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

# OS-36: Investigate how to rewrite docker.py to support AKS
Docker Flow:
1. check if job container created, skip the job if already created
2. create volume and a busybox container for the volume (for file management)
3. git checkout study repo
4. copy code into volume
5. check if job image exist (e.g. cohort extractor)
6. run the job with volume
7. monitor the status of the job until it is done
8. use busybox container to find matching output files
9. write docker metadata and job stdout to log folder
10. copy output files out from volume
11. (force) delete job container, busybox container and volume

k8s Flow:
1. create pv and pvc (ws_pvc) for the workspace if not exist
2. check if the job exists, skip the job if already created
3. create pv and pvc (job_pvc) for the job
4. create a k8s job with ws_pvc and job_pvc mounted, this job consists of multiple steps running in multiple containers: 
   1. pre container: git checkout study repo to job volume
   2. job container: run the opensafely job command (e.g. cohortextractor) on job_volume
   3. post container: use python re to move matching output files from job volume to ws volume
5. monitor the status of the k8s job until it is done
6. write stdout of k8s job to log folder in ws volume
7. (force) delete job, job pv and job pvc, but keep the ws pv and ws pvc
