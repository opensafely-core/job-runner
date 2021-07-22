# TPP backend

Use git-bash as your terminal

All job-runner code, logs, files etc are in: `/e/job-runner`


## Operations

### Windows Service

#### Starting/stopping the service

job-runner is designed to be safe to interrupt at any point, however due
to a [bug][1] in Docker killing a copy command at the wrong point can
put a container into a permanently hung state. This is not a disaster as
we now detect this condition, mark the job as failed and ignore it. But
it will require the researcher to re-run their job, and it leaves a
frozen container hanging around until next time Docker is restarted, so
it's best avoided if possible.

To do this tail the log file and look at the most recent `run` line:

    tail service.err.log

If the line looks like either of the below lines that means the
job-runner is currently copying files into a container and is best not
interrupted:

    2021-07-21 18:54:58.792Z run  Copying in code from ...
    2021-07-21 18:54:59.131Z run  Copying input file ...

Anything else is safe to interrupt and the service can be stopped with:

    /e/bin/nssm stop opensafely

The service can be restarted with:

    /e/bin/nssm start opensafely

Note that nssm will warn about "Unexpected status SERVICE_START_PENDING"
because we don't start fast enough. This can be ignored.

You can check the service has started OK by following the logs for a
bit:

    tail -f service.err.log

And you can check its running status with:

    /e/bin/nssm status opensafely

To view issues with starting/stopping the windows service, the following will
launch Windows Event Viewer directly pointing at the nssm events.

    /e/bin/events.sh


[1]: https://github.com/docker/for-mac/issues/4491


#### Viewing job-runner logs

The main log file is:

    /e/job-runner/service.err.log

We also log stdout to `/e/job-runner/service.log` but that is usually
empty.

These files are rotated by nssm.


#### Configuring the job-runner

All configuration is via environment variables set in the `.env`
file.

For instance, to enable DEBUG level logging add the following line to
the `.env` file:

    LOGLEVEL=DEBUG

And then restart the job-runner.


#### Update job-runner

In `/e/job-runner` run:

    git pull


Then restart the service


### Generic operations

#### Update docker image

    ./scripts/update-docker-image.sh image[:tag]

#### View specific job logs

    ./scripts/watch-job-logs.sh

This will let you choose a job's output to tail.

Supply a string argument to filter to just job names matching that
string. If there is only one match it will automatically select that
job.


#### Mount the volume of a running job

    ./scripts/mount-job-volume.sh

Starts a container with the volume associated with a given job mounted
at `/workspace`.

Supply a string argument to filter to just job names matching that
string. If there is only one match it will automatically select that
job.

Note that the container will be a privileged "tools" container suitable
for stracing (see below).


#### stracing a running job

Start a privileged container which can see other containers processes:

    docker run --rm -it --privileged --pid=host ghcr.io/opensafely-core/tools

Find the pid of the relevent process inside the job in question:

    ps faux | less

Strace it:

    strace -fyp <pid>


### Retrying a job which failed with "Internal error"

When a job fails with the message "Internal error" this means that
something unexpected happened and an exception other than JobError was
raised. This can be a bug in our code, or something unexpected in the
environment. (Windows has sometimes given us an "I/O Error" on
perfectly normal file operations.)

When this happens the job's container and volume are not
automatically cleaned up and so it's possible to retry the job without
having to start from scratch. You can run this with:

    bash scripts/run.sh -m jobrunner.retry_job <job_id>

The `job_id` actually only has to be a sub-string of the job ID (full
ones are a bit awkward to type) and you will be able to select the
correct job if there are multiple matches.


### Killing a job

To kill a running job (or prevent it starting if it hasn't yet) use the
`kill_job` command:

    bash scripts/run.sh -m jobrunner.kill_job --cleanup <job_id> [... <job_id>]

The `job_id` actually only has to be a sub-string of the job ID (full
ones are a bit awkward to type) and you wil be able to select the
correct job if there are multiple matches.

Multiple job IDs can be supplied to kill multiple jobs simultaneously.

The `--cleanup` flag deletes any associated containers and volumes,
which is generally what you want.

If you want to kill a job but leave the container and volume in place
for debugging then omit this flag.

The command is idempotent so you can always run it again later with the
`--cleanup` flag.


### Clearing up diskspace

To view current disk usage:

    docker system df -v

Generally, only running containers are doing anything useful. Stopped
containers are usually safe to delete.  To clean up stopped containers:

    docker containers prune

Typically, it will be orphaned volumes that will be taking up space. To delete
orphaned volumes that have no associated container:

    docker volume prune
