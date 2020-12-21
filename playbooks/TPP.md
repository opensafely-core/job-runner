# TPP backend

Use git-bash as your terminal

Service location: /e/job-runner

## Operations

Note: as of 2020-12-18, we are using the docker based method. Come early
January, we expect to switch to windows service.

### Docker compose (current)

#### Starting/stopping the service

    docker-compose up -d

    docker-compose stop

#### Viewing logs

    docker-compose logs -t | less -R

### Windows Service (future)


#### Starting/stopping the service

    /c/nssm-2.4/win64/nssm stop opensafely
    /c/nssm-2.4/win64/nssm start opensafely
    /c/nssm-2.4/win64/nssm status opensafely

Note: start command gives spurious warning, ignore.

To view issues with starting/stopping the windows service, the following will
launch Windows Event Viewer directly pointing at the nssm events.

    /e/bin/events.sh

#### Viewing job-runner logs

stdout is in /e/job-runner/service.log
stderr is in /e/job-runner/service.err.log

These files are rotated by nssm.

TODO: combine these into one log?

### Generic operations

#### Update docker image

    ./scripts/update-docker-image.sh image[:tag]

#### Update the job-runner itself

    ./scripts/update-docker-image.sh job-runner

And restart.

#### View specific job logs

    /e/bin/watch-job-logs.sh

This will let you choose a job's output to tail.

Supply a string argument to filter to just job names matching that
string. If there is only one match it will automatically select that
job.


### Retrying a job which failed with "Internal error"

When a job fails with the message "Internal error" this means that
something unexpected happened and an exception other than JobError was
raised. This can be a bug in our code, or something unexpected in the
environment. For instance, Windows sometimes gives us an "I/O Error" on
perfectly normal file operations (we suspect this is due to the
docker-in-docker setup).

When this happens the job's container and volume are not
automatically cleaned up and so it's possible to retry the job without
having to start from scratch. You can run this with:

    docker-compose run --rm jobrunner-run python -m jobrunner.retry_job <job_id>

The `job_id` actually only has to be a sub-string of the job ID (full
ones are a bit awkward to type) and you wil be able to select the
correct job if there are multiple matches.
