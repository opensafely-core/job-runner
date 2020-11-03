# Developer notes

This package has two main entrypoints: `jobrunner.sync` and
`jobrunner.run`. Both are implemented as infinite loops with a fixed
sleep period and are designed to be run as services.


## jobrunner.sync

This handles all communication between the job-server and the
job-runner. It polls the job-server for active JobRequests, updates its
local Jobs table accordingly, and then posts back the details of all
Jobs associated with the active JobRequests it received.

The bulk of the work here is done by the `create_or_update_jobs` module.


## jobrunner.run

This runs Docker containers based on the contents of the Jobs table.
It's implemented as a synchronous loop which polls the database for
active jobs and takes appropriate actions.

The bulk of the work here is done by the `manage_jobs` module which
starts new Docker containers and stores the appropriate outputs when
they finish.


## Database layer

The `models` module contains the data structures representing
JobRequests and Jobs. We use simple, dumb `dataclasses`.

In order to reduce external dependencies to a minimum we have our own
very crude ORM-like layer in the `database` module.
