# TPP backend

Use git-bash as your terminal

Service location: /e/job-runner

## Operations

### Starting/stopping the service

    docker-compose up -d

    docker-compose stop

### Viewing logs

    docker-compose logs -t | less -R

### Update docker image

    ./scripts/update-docker-image.sh image[:tag]

### Update the job-runner itself

    ./scripts/update-docker-image.sh job-runner
    docker-compose down
    docker-compose up -d

### View specific job logs

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
