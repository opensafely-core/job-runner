# Developer notes



## Prerequisites for local development

### Just

We use [`just`](https://github.com/casey/just) as our command runner. It's
a single file binary available for many platforms so should be easy to
install.

```sh
# macOS
brew install just

# Linux
# Install from https://github.com/casey/just/releases

# Add completion for your shell. E.g. for bash:
source <(just --completions bash)

# Show all available commands
just #  shortcut for just --list
```

### Python

You'll need an appropriate version of Python on your PATH. Check the
`.python-version` file for the required version.

### Docker

You will also need an up-to-date version of Docker Compose. Instructions to install it are [here](https://docs.docker.com/compose/install/).


## Getting started

Set up a local development environment with:
```
just devenv
```

This includes the production dependencies in `requirements.txt` (which
are intentionally kept minimal) and the dev dependencies in `requirements.dev.txt`.

It also creates a `.env` file from `dotenv-sample`, and populates the
`HIGH_PRIVACY_STORAGE_BASE` and `MEDIUM_PRIVACY_STORAGE_BASE` environment
variables.

Update `.env` to add a value for `PRIVATE_REPO_ACCESS_TOKEN`; this should be a
developer [GitHub PAT](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#about-personal-access-tokens) with `repo` scope.


## Architecture

The package has two main entrypoints:
[jobrunner.sync](./jobrunner/sync.py) and
[jobrunner.run](./jobrunner/run.py). Both are implemented as infinite
loops with a fixed sleep period and are designed to be run as services.

### jobrunner.sync

This handles all communication between the job-server and the
job-runner. It polls the job-server for active JobRequests, updates its
local Jobs table accordingly, and then posts back the details of all
Jobs associated with the active JobRequests it received.

The bulk of the work here is done by the
[create_or_update_jobs](./jobrunner/create_or_update_jobs.py) module.

### jobrunner.run

This runs Docker containers based on the contents of the Jobs table.
It's implemented as a synchronous loop which polls the database for
active jobs and takes appropriate actions.

The bulk of the work here is done by the the [local Docker executor
implementation](./jobrunner/executors/local.py) module which starts new Docker
containers and stores the appropriate outputs when they finish.

### Job State

Jobs move through a defined set of `StatusCode`'s as the job-runner manages them.
These are defined in `jobrunner/models.py`.

The diagram below shows the transitions, but all states have an implicit transition to
`INTERNAL_ERROR` or `CANCELLED_BY_USER`, which is not shown.


```mermaid
graph TD
    CREATED --> PREPARING
    CREATED --> WAITING_ON_DEPENDENCIES
    CREATED --> WAITING_ON_WORKERS
    CREATED --> WAITING_ON_REBOOT
    CREATED --> WAITING_DB_MAINTENANCE
    CREATED --> WAITING_PAUSED
    CREATED --> STALE_CODELISTS
    CREATED --> SUCCEEDED
    WAITING_ON_DEPENDENCIES -->  WAITING_ON_WORKERS
    WAITING_ON_DEPENDENCIES -->  WAITING_ON_REBOOT
    WAITING_ON_DEPENDENCIES -->  WAITING_DB_MAINTENANCE
    WAITING_ON_DEPENDENCIES --> PREPARING
    WAITING_ON_DEPENDENCIES --> DEPENDENCY_FAILED
    WAITING_PAUSED --> PREPARING
    WAITING_ON_WORKERS --> PREPARING
    WAITING_ON_REBOOT --> PREPARING
    WAITING_DB_MAINTENANCE --> PREPARING
    PREPARING --> EXECUTING
    EXECUTING --> FINALIZING
    FINALIZING --> SUCCEEDED
    FINALIZING --> NONZERO_EXIT
    FINALIZING --> UNMATCHED_PATTERNS
    FINALIZING --all states can go here--> CANCELLED_BY_USER
    FINALIZING --all states can go here--> INTERNAL_ERROR
    FINALIZING --all states can go here--> KILLED_BY_ADMIN

    subgraph Legend
      direction TB
      LEGEND_ERROR[ERROR STATE]
      LEGEND_BLOCKED[BLOCKED]
      LEGEND_NORMAL[HAPPY PATH]
    end

    %% styles
    classDef default fill:#00397a,color:#f1f7ff,stroke-width:2px,stroke:#002147;
    classDef error fill:#b6093d,color:#fef3f6,stroke-width:2px,stroke:#770628;
    classDef blocking fill:#ffdf75,color:#7d661c,stroke-width:2px,stroke:#997d23;

    class LEGEND_BLOCKED,WAITING_ON_WORKERS,WAITING_ON_REBOOT,WAITING_PAUSED,WAITING_DB_MAINTENANCE blocking
    class LEGEND_ERROR,INTERNAL_ERROR,UNMATCHED_PATTERNS,DEPENDENCY_FAILED,NONZERO_EXIT,STALE_CODELISTS error

```

## Testing


Tests can be run with:

    just test

Some of these tests involve talking to GitHub and there is a big fat
integration test which takes a while to run. You can run just the fast
tests with:

    just test-fast

The big integration test will sit there inscrutably for 30s-1min.
If you want to know what it's up to you can get pytest to show the log output with:

    just test-verbose

### Testing in docker

To run tests in docker, simply run:

    just docker/test

This will build the docker image and run tests. You can run job-runner as
a service with:

    just docker/service

Or run a command inside the docker image:

    just docker/run ARGS=command  # bash by default


## Running jobs locally

Adding jobs locally is most easily done with the `just add-job` command, which
calls `jobrunner.cli.add_job` with a study repo and an action to run e.g.
```
just add-job https://github.com/opensafely/os-demo-research run_all
```

As well as URLs this will accept paths to local git repos e.g.
```
just add-job ../os-demo-research run_all
```

You can now run the main loop and you'll see it pick up the jobs:
```
just run
```

See the full set of options `add-job` will accept with:
```
python -m jobrunner.cli.add_job --help
```

## Running jobs on the test backend

The [test backend](https://github.com/opensafely-core/backend-server/tree/main/backends/test) is
a test version of an OpenSAFELY backend which has no access to patient data, but can be used to
schedule and run jobs in a production-like environment.

You will need ssh access to test.opensafely.org in order to add jobs using the CLI. This
currently requires the same permissions as any non-test backend; see the
[developer permissions documentation](https://bennett.wiki/products/developer-permissions-log/#platform-developerstesters) for further details.

```
ssh <your-username>@test.opensafely.org
sudo su - opensafely

just jobrunner/add-job https://github.com/opensafely/os-demo-research run_all
```

You will see the output of the newly created job (note that if it returns `'state': 'succeeded'`
in the displayed json, the job has already run successfully on the test backend. Use `-f` to
force dependencies to re-run).

The jobrunner service is already running in the background on the test backend, so
jobs should be picked up and run automatically. Check the job logs to see the progress of your
job. From the output of `just add-job`, find the new job's `id` value.

Now check the logs for this job:

```
just jobrunner/logs-id <your-job-id>
```

## job-runner docker image

Building the docker image:

    just docker/build                   # build base and dev image
    just docker/build prod              # build base and prod image


## Database schema and migrations

jobrunner uses a minimal ORM-lite wrapper to talk to the DB.

The current version of a tables schema definition is stored in the the
`__tableschema__` attribute for that model's class, i.e. `Job.__tableschema__`.
This is use to create the table in dev and test, so migrations are not usually
needed in those cases.

### Adding a migration

However, we also occasionally need to apply changes to this schema in
production, or in a user's local opensafely-cli database.

To do this, we track migrations in `jobrunner/models.py`. Add a migration like so:

```python
database.migration(1, """
DDL STATEMENT 1;
DDL STATEMENT 2;
""")
```

These statements are run together in a single transaction, along with
incrementing the `user_version` in the database.

Note: be aware that there are various restrictions on ALTER TABLE statements in
sqlite:

https://www.sqlite.org/lang_altertable.html#alter_table_add_column


### Applying migrations


Trying to run jobrunner as a service will error if the database does not exist
or is out of date, as a protection against misconfiguration.

To initialise or migrate the database, you can use the migrate command:

```sh
just migrate
```

## Deploying

The jobrunner docker image is built by GitHub actions on merges to `main` and deployed automatically
on backend servers.
