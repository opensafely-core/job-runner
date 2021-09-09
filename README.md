# OpenSAFELY job runner

A job runner is a service that encapsulates:
 * the task of checking out an OpenSAFELY study repo;
 * executing actions defined in its `project.yaml` configuration file when
   requested via a jobs queue; and
 * storing its results in a particular locations.


Quickrefs:
 - [Playbooks](playbooks)

End users will find more information in the [OpenSAFELY documentation](https://docs.opensafely.org).

# Operating principles

In production, this software runs as a loop on a secure server within the
infrastructure of the primary data provider.  It polls an [OpenSAFELY job
server](https://github.com/opensafely-core/job-server), looking for requests to run
jobs.

Jobs belong to a `workspace`. This describes the git repo containing the
OpenSAFELY-compliant project under execution; the git branch, and kind of
database to use. The workspace also acts as a kind of namespace for
partitioning outputs of its jobs.

An OpenSAFELY-compliant repo must provide a `project.yaml` file which
describes how a requested job should be converted into a command (& arguments)
that can be run in a subprocess on the secure server.  It incorporates the idea
of dependencies, so an action that generates a chart might depend on an action
that extracts data from the database *for* that chart.  See the
[Actions reference](https://docs.opensafely.org/actions-intro/) for more information.

An action can define `outputs`; these are persisted on disk and made available
to subsequent actions in the workspace, and users who have permission to log
into the server and view the raw files.

The runner takes care of executing dependencies in order. By default, it skips
re-running a dependency whose previous run produced output that still exists in
the production environment.  The runner also reports status back to the job
server, redacting possibly-sensitive information.

The runner is bundled as part of the [opensafely-cli][cli] tool so users
can test their actions locally.

[cli]: https://github.com/opensafely-core/opensafely-cli

## Job structure

The job server serves jobs as JSON in the following format. First, a job must
belong to a workspace:

```json
{
    "workspace": {
        "name": "my workspace",
        "repo": "https://github.com/opensafely/job-integration-tests",
        "branch": "master",
        "db": "full"
    }
}
```
Possible values for `"db"` are "full", "slice", and "dummy".

A workspace is a way of associating jobs related to a given combination of
branch, repository and database. To enqueue a job, a client POSTs JSON like
this:

```json
{
    "backend": "tpp",
    "action_id": "do_thing",
    "workspace_id": 1
}
```

## Consuming jobs

A job runner is service installed on a machine that has access to a given
backend. It receives jobs from the server and consumes those whose `backend` value matches the
value of the current `BACKEND` environment variable.

It must also define three environment variables which are an RFC1838 connection
URL; these correspond to the `db` requested in the job's workspace definition,
and as such are named `FULL_DATABASE_URL`, `SLICE_DATABASE_URL`, and
`DUMMY_DATABASE_URL`.

When a job is found, the following happens:

* The corresponding repo is fetched. Private repos are accessed using
  the `PRIVATE_REPO_ACCESS_TOKEN` supplied in the environment.
* Its `project.yaml` is parsed:
  * Individual `actions` are extracted from this file
  * A dependency graph is calculated for the requested action; for example, an
    action might depend on three previous actions before it can be run
  * Each action in the graph is checked to see if it needs to be run
    * Actions that either: (a) already have output generated from a previous
      run; (b) are currently running; (c) failed on their last run do not
      need to be run
  * If a dependency has failed, then the requested action fails
  * If the dependency needs to be run, a new job is pushed to the queue, and the
    current job is postponed
  * If an action has no dependencies needing to be run, then its `docker run` is
    executed
  * On completion, a status code and message are reported back to the job
    server. On success, a list of output file locations are also posted. On
    failure, the message has any potentially-sensitive information redacted, and is associated with
    a unique string so that a user with requisite permissions can log into the
    production environment and examine the docker logs for the full error.

## Output locations

Every action defines a list of `outputs` which are persisted to a permanent
storage location.  The project author must categorise these outputs as either
`highly_sensitive` or `moderately_sensitive`.  Any pseudonymised data which may
be highly disclosive (e.g. without low number redaction) should be classed as
`highly_sensitive`; data which the author believes could be released following
review should be classed as `moderately_sensitive`. This design allows tiered
levels of permissions for collaborators to review data outputs. For example, the
study author would usually have access to `highly_sensitive` material for
debugging; but other collaborators could have access to `moderately_sensitive`
data to prepare it for release (for which it is planned to add a
`minimally_sensitive` category).

Outputs are therefore persisted to filesystem paths according to the following
environment variables:

```sh
# A location where cohort CSVs (one row per patient) should be
# stored. This folder must exist.
OPENSAFELY_HIGH_PRIVACY_STORAGE_BASE=/home/opensafely/high_security

# A location where script outputs (some for publication) should be
# stored
OPENSAFELY_MEDIUM_PRIVACY_STORAGE_BASE=/tmp/outputs/medium_security
```
## Project.yaml

A valid project file looks like this:

```yaml
version: "3.0"

expectations:
  population_size: 1000

actions:

  generate_study_population:
    run: cohortextractor:latest generate_cohort --study-definition study_definition
    outputs:
      highly_sensitive:
        cohort: output/input.csv

  run_model:
    run: stata-mp:latest analysis/model.do
    needs: [generate_study_population]
    outputs:
      moderately_sensitive:
        model: models/cox-model.txt
        figure: figures/survival-plot.png
```
See the [project pipeline documentation](https://docs.opensafely.org/actions-pipelines/) for a detailed
description of the project.yaml setup.


## Local actions development

The [`cohortextractor` command-line tool](https://github.com/opensafely/cohort-extractor/) imports this library, and implements the action-parsing-and-running functionality as a series of
synchronous docker commands, rather than asynchronously via the job queue.


# For developers

Please see [the additional information](DEVELOPERS.md).
