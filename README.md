A job runner is a service that encapsulates:
 * the task of checking out an OpenSAFELY study repo;
 * executing actions defined in its `project.yaml` configuration file when
   requested via a jobs queue; and
 * storing its results in a particular locations.

# Overview

This documentation is aimed at developers looking for an overview of how the
system works.  It also has some parts relevant for end users, particularly the
`project.yaml` documentation.

# Operating principles

In production, this software runs as a loop on a secure server within the
infrastructure of the primary data provider.  It polls an [OpenSAFELY job
server](https://github.com/opensafely/job-server), looking for requests to run
jobs.

Jobs belong to a `workspace`. This describes the git repo containing the
OpenSAFELY-compliant project under execution; the git branch, and kind of
database to use. The workspace also acts as a kind of namespace for
partitioning outputs of its jobs.

An OpenSAFELY-compliant project must provide a `project.yaml` file which
describes how a requested job should be converted into a command (& arguments)
that can be run in a subprocess on the secure server.  It incorporates the idea
of dependencies, so an action that generates a chart might depend on an action
that extracts data from the database *for* that chart.

An action can define `outputs`; these are persisted on disk and made available
to subsequent actions in the workspace, and users who have permission to log
into the server and view the raw files.

The runner takes care of executing dependencies in order. By default, it skips
re-running a dependency whose previous run produced output that still exists in
the production environment.  The runner also reports status back to the job
server, redacting possibly-sensitive information.

The runner is also imported by the `cohortextractor` tool, so users can test
their actions locally.

## Job structure

The job server serves jobs as JSON in the following format. First, a job must
belong to a workspace:

```json
{
    "workspace": {
        "name": "my workspace",
        "repo": "https://github.com/opensafely/job-integration-tests",
        "branch": "master",
        "db": "full"  # possible values: `full`, `slice`, and `dummy`
    }
```

A workspace is a way of associating jobs related to a given combination of
branch, repository and database. To enqueue a job, a client POSTs JSON like
this:

```json
{
    "backend": "tpp",
    "action_id": "do_thing",
    "workspace_id": 1,
}
```

## Consuming jobs

A job runner is service installed on a machine that has access to a given
backend. It consumes jobs from the server whose `backend` value matches the
value of the current `BACKEND` environment variable.

It must also define three environment variables which are an RFC1838 connection
URL; these correspond to the `db` requested in the job's workspace definition,
and as such are named `FULL_DATABASE_URL`, `SLICE_DATABASE_URL`, and
`DUMMY_DATABASE_URL`.

When a job is found, the following happens:

* The corresponding repo is checked out to a temporary folder. Private repos are
  accessed using the `PRIVATE_REPO_ACCESS_TOKEN` supplied in the environment.
* Its `project.yaml` is parsed:
  * Individual `actions` are extracted from this file
  * Variables in each action are interpolated, and the `run` action is converted
    to a `docker run` invocation
  * A dependency graph is calculated for the requested action; for example, an
    action might depend on three previous actions before it can be run
  * Each action in the graph is checked to see if it needs to be run
    * Actions that either: (a) already have output generated from a previous
      run; (b) are currently running; (c) failed on their last run to do not
      need to be run
  * If a dependency has failed, then the requested action fails
  * If the dependency needs to be run, a new job is pushed to the queue, and the
    current job is postponed
  * If an action has no dependencies needing to be run, then its `docker run` is
    executed
  * On completion, a status code and message are reported back to the job
    server. On success, a list of output file locations are also posted. On
    failure, the message has any potentially-sensitve information redacted, and
    a unique string so that a user with requisite permissions can log into the
    production environment and examine the docker logs for the full error.

## Output locations

Every action defines a list of `outputs` which are persisted to a permanent
storage location.  The project author must categorise these outputs as either
`highly_sensitive` or `moderately_sensitive`.  Any pseudonomised data which may
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
## Project.yaml description

A valid project file looks like this:

```yaml
version: '1.0'

actions:

  generate_cohort:
    run: cohortextractor:0.5.2 --output-dir=/workspace
    outputs:
      highly_sensitive:
        cohort: input.csv

  run_model:
    run: stata-mp:latest analysis/model.do ${{ needs.generate_cohorts.outputs.highly_sensitive.cohort }}
    needs: [generate_cohorts]
    outputs:
      moderately_sensitive:
        log: model.log
```

`version` must currently always be `1.0`, and refers to the version of the project.yaml syntax used in the file.

`actions` is a list of actions required to run the entire project end-to-end. Each action must have a run command, which is of the format `<command>:<version> <arguments>`. The currently-supported commands are `cohortextractor`, `r` and `stata-mp`.

The <version> must correspond to a published docker tag. Available tags for cohortextractor are [here](https://github.com/opensafely/cohort-extractor/tags), and correspond to the versions of the cohortextractor tool that you see if you run `cohortextractor --version` from the command line. For the scripting images (`r`, `stata-mp`, you should always specify `latest` - for the time being.

Each action has a list of `outputs` which are copied to an appropriately secure location available to subsequent steps (and to users logged into the secure environment with the relevant permissions). These are of the form <action_id>: <filename>. The run command must produce files in the current directory that correspond with these filenames.

Each action can also refer to other actions with the `needs` field. This is a list of actions that must complete successfully before the given action can run.

An action that `needs` other actions can refer to the `outputs` of previous actions using the form `${{ needs.<action_id>.outputs.<output_id> }}`. This is substituted with a path to the file in question.

As such, a script that reads an input file needs to refer to its location as a command line argument. In the example above, a `stata` do-file would open a CSV like this:

```do
args csv
import delimited `csv'
```


## Local actions development

The `[cohortextractor` command-line tool](https://github.com/opensafely/cohort-extractor/) imports this library, and implements the action-parsing-and-running functionality as a series of
synchronous docker commands, rather than asychronously via the job queue.


# Installation

Each instance of a runner is expected to consume jobs for just one backend (e.g.
`tpp`); this is one of the required environment variables.

To run a server in watch mode, copy `dotenv-sample` to `.env` and edit its
values; then

    docker-compose up


The image is published automatically to our docker registry whenever branches are
merged.  To build the image locally:

    docker-compose build

There are [integration tests in a separate
repo](https://github.com/opensafely/job-integration-tests) to check interaction
between the job server and the job runner

## Docker

The job runner must have all the docker images mentioned in the dict
[`RUN_COMMANDS_CONFIG`](./jobrunner/project.py) pulled and up-to-date; project
authors can specify an image tag, so new releases will need to be pulled
manually as needed.
