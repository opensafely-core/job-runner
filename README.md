A job runner is a service that encapsulates:
 * the task of checking out an OpenSAFELY study repo;
 * executing actions defined in its `project.yaml` configuration file when
   requested via a jobs queue; and
 * storing its results in a particular locations.


Quickrefs:
 - [Playbooks](playbooks)


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

* The corresponding repo is fetched. Private repos are accessed using
  the `PRIVATE_REPO_ACCESS_TOKEN` supplied in the environment.
* Its `project.yaml` is parsed:
  * Individual `actions` are extracted from this file
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

`version` refers to the version of the project.yaml syntax used in the file (currently supported are 1.0, 2.0, 3.0).

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


# DEVELOPER NOTES

Install the development dependencies with:
```
pip install -r requirements.dev.txt
```
This includes the production dependencies in `requirements.txt` which
are intentionally kept minimal.

The package has two main entrypoints:
[jobrunner.sync](./jobrunner/sync.py) and
[jobrunner.run](./jobrunner/run.py). Both are implemented as infinite
loops with a fixed sleep period and are designed to be run as services.


## jobrunner.sync

This handles all communication between the job-server and the
job-runner. It polls the job-server for active JobRequests, updates its
local Jobs table accordingly, and then posts back the details of all
Jobs associated with the active JobRequests it received.

The bulk of the work here is done by the
[create_or_update_jobs](./jobrunner/create_or_update_jobs.py) module.


## jobrunner.run

This runs Docker containers based on the contents of the Jobs table.
It's implemented as a synchronous loop which polls the database for
active jobs and takes appropriate actions.

The bulk of the work here is done by the
[manage_jobs](./jobrunner/manage_jobs.py) module which starts new Docker
containers and stores the appropriate outputs when they finish.


## Testing

Tests can be run with:
```
python -m pytest
```
(Note that the `pytest` command is subtly different and won't work).

Some of these tests involve talking to GitHub and there is a big fat
integration test which takes a while to run. You can run just the fast
tests with:
```
python -m pytest -m "not slow_test"
```

The big integration test will sit there inscrutably for 30s-1min. If you
want to know what it's up to you can get pytest to show the log output
with:
```
python -m pytest tests/test_integration.py -o log_cli=true -o log_cli_level=INFO
```

### Testing on Windows

For reasons outlined in [#76](https://github.com/opensafely/job-runner/issues/76) this
is a bit painful. None of the tests which require talking to Docker are
run in CI. However it is possible to run them locally assuming you have
Windows installed in a VM and Docker running on the host. The steps are:

1. Install git in the Windows VM: https://git-scm.com/download/win

2. Install Python 3.7 in the Windows VM:
   I used Python 3.7.9 [Windows x86-64 executable](https://www.python.org/ftp/python/3.7.9/python-3.7.9-amd64.exe) installer from:
   https://www.python.org/downloads/windows/

3. On the host machine, navigate to your job-runner checkout and run
   ```sh
   ./scripts/host-services-for-win-testing.sh
   ```
   This will let you VM talk to host Docker and fetch stuff from your
   git repo so you don't need to push to github to test locally.
   (Note you'll need `socat` installed.)

4. Inside the VM, open a git-bash shell and run:
   ```sh
   git clone git://10.0.2.2:8343/ job-runner
   cd job-runner
   ./scripts/run-tests-in-windows.sh
   ```
   `10.0.2.2` is the default NAT gateway in Virtualbox. Port 8343 is the
   where we set our git-daemon to listen on.

   This will (or should) create a virtualenv, install the requirements,
   download the [docker cli](https://github.com/StefanScherer/docker-cli-builder/)
   (not the full Docker package), and run the tests using the host
   Docker daemon.


## Running jobs locally

Adding jobs locally is most easily done with the `jobrunner.add_job`
command e.g
```
python -m jobrunner.add_job https://github.com/opensafely/os-demo-research run_all
```

As well as URLs this will accept paths to local git repos e.g.
```
python -m jobrunner.add_job ../os-demo-research run_all
```

If you now run the main loop you'll see it pick up the jobs:
```
python -m jobrunner.run
```

See the full set of options it accepts with:
```
python -m jobrunner.add_job --help
```
