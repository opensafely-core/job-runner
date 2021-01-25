# TPP backend

Use git-bash as your terminal

Service location: /e/job-runner

## Operations

### Windows Service

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

#### View specific job logs

    /e/bin/watch-job-logs.sh

This will let you choose a job's output to tail.

Supply a string argument to filter to just job names matching that
string. If there is only one match it will automatically select that
job.


#### stracing a running job

Start a privileged container which can see other containers processes:

    docker run --rm -it --privileged --pid=host ghcr.io/opensafely/tools

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
