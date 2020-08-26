A runner which encapsulates the task of checking out an OpenSAFELY study repo;
executing actions defined in its `project.yaml` configuration file; and  sdf
storing its results in a particular locations.

# Overview

## Producing jobs

A client requests that a job is run by pushing JSON to an endpoint on an
[OpenSAFELY job server](https://github.com/opensafely/job-server).

If one doesn't already exist, the client must create a workspace, thus:

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
    "operation": "do_thing",
    "workspace_id": 1,
}
```

In practice, jobs are (will be) added to the queue via a web interface on the
job server.

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
* Its `project.yaml` is parsed
  * Individual `actions` are extracted
  * Variables in each action are interpolated, and the `run` action is converted
    to a `docker run` invocation
  * A dependency graph is calculated for the requested action; for example, an
    action might depend on three previous actions before it can be run
  * Each action in the graph is checked to see if it needs to be run
    * Actions that either: (a) already have output generated from a previous
      run; (b) are currently running; (c) failed on their last run to do not
      need to be run
  * If the last run of a dependency failed, then the requested action fails
  * If the dependency needs to be run, a new job is pushed to the queue, and the
    current job is postponed
  * If an action has no dependencies needing to be run, then its `docker run` is
    executed
  * On success, a status code and message is reported back to the job server,
    along with a list of output file locations (q.v.); on failure, a status code
    and message is reported back to the server, with any potentially disclosive
    information redacted, and a unique string so that a user with requisite
    permissions can log into the production environment and examine the docker
    logs for the full error.

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

`actions` is a list of actions required to run the entire project end-to-end. Each action must have a run command, which is of the format <command>:<version> <arguments>. The currently-supported commands are `cohortextractor`, `r` and `stata-mp`.

The <version> must correspond to a published docker tag. Available tags for cohortextractor are [here](https://github.com/opensafely/cohort-extractor/tags), and correspond to the versions of the cohortextractor tool that you see if you run `cohortextractor --version` from the command line. For the scripting images (`r`, `stata-mp`, you should always specify `latest` - for the time being.

Each action has a list of `outputs` which are copied to an appropriately secure location available to subsequent steps (and to users logged into the secure environment with the relevant permissions). These are of the form <action_id>: <filename>. The run command must produce files in the current directory that correspond with these filenames.

Each action can also refer to other actions with the `needs` field. This is a list of actions that must complete successfully before the given action can run.

An action that `needs` other actions can refer to the `outputs` of previous actions using the form `${{ needs.<action_id>.outputs.<output_id> }}`. This is substituted with a path to the file in question.

As such, a script that reads an input file needs to refer to its location as a command line argument. In the example above, a `stata` do-file would open a CSV like this:

```do
args csv
import delimited `csv'
```


## Command-line testing

The `[cohortextractor` command-line
tool](https://github.com/opensafely/cohort-extractor/) imports this library, and
implements the action-parsing-and-running functionality as a series of
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

The job runner must have all the docker images mentioned in
`project.RUN_COMMANDS_CONFIG` pulled and up-to-date; project authors can specify
an image tag, so new releases will need to be pulled manually as needed.
