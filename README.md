# OpenSAFELY job runner

This repository contains three services that form part of the wider [OpenSAFELY](https://docs.opensafely.org/) system:
* the RAP Controller
* the RAP Agent
* the RAP Controller API

Together, these services do things such as:
 * retrieving jobs to be run from an [OpenSAFELY job
server](https://github.com/opensafely-core/job-server);
 * cloning an OpenSAFELY study repo from Github;
 * executing actions defined in a workspace's `project.yaml` configuration file when
   requested via a jobs queue; and
 * storing the results of a job in particular locations.

The [technical architecture container diagram](https://docs.opensafely.org/technical-architecture/#container-diagram) shows these services in context.

# For developers

Please see [the additional information for developers](DEVELOPERS.md) and [the deployment notes](DEPLOY.md).
