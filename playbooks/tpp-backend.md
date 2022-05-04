# TPP Backend (DEPRECATED)

THIS PLAYBOOK HAS BEEN DEPRECATED

The general backend playbook is at https://github.com/opensafely-core/backend-server/blob/main/jobrunner/playbook.md#jobrunner


## Fundamentals

Use git-bash as your terminal.

job-runner is run directly from a git checkout of the code in:

    /e/job-runner

Interesting files/directories include:

    service.err.log - main log file

    scripts - directory with useful scripts

    .env - environment variables for configuration (added to job-runner's env by run.sh script)

    lib - directory with job-runner's depenencies (added to PYTHONPATH by run.sh script)

    workdir/db.sqlite - the main job-runner database
    workdir/stats.sqlite - resource usage stats for jobs
    workdir/repos - partial clones of study repos (we only pull commits we need)

Other useful paths are:

    /e/high_privacy/workspaces - contains workspace directories

    /e/high_privacy/logs - contains logs and metadata for every job we've run, organised into YYYY-MM subdirs

    /e/FILESFORL4/workspaces - copy of workspaces containing only medium (and lower) privacy files

job-runner is run as the `opensafely` service by the Windows service
management tool [NSSM](https://nssm.cc/) which invokes it using the
following command:

    bash scripts/run.sh -m jobrunner.service

You can interact with NSSM using the `/e/bin/nssm` executable e.g

    /e/bin/nssm status opensafely

**Note on job IDs:**
Because there is no option to copy/paste in or out of the secure
environment all commands below which involve job IDs (or job request
IDs) will also accept a partial fragment of an ID. So intead of having
to type `dh6m4ocmvdzzkpgq` you could just type `dh6` or `ocmv`. If
there's a unique match the command will continue as normal. If there are
multiple matches than the command will ask you to choose between them.


## Common tasks

### Restarting the service

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


### Updating job-runner

In `/e/job-runner` run:

    git pull

Then restart the service.


### Viewing job-runner logs

The main log file is:

    /e/job-runner/service.err.log

We also log stdout to `/e/job-runner/service.log` but that is usually
empty. Both these files are rotated by nssm.

Use `tail -f` to see what's happening right now:

    tail -f service.err.log

Lines prefixed `sync` are from the synchronisation loop which
communicates with the jobs website i.e. polls for new jobs to do and
pushes the state of current jobs back up.

Lines prefixed `run` are from the run loop which deals with starting,
checking up on and stopping Docker containers.

If you are looking for information on a specific job or job request then
open the log with `less` and search for a few characters from the ID.
Usually four characters will get you uniqueness. (Use `less` rather than
`grep` so you get the full error messages and context.)


### Pulling a docker image

You can't run `docker pull` as normal because all Docker operations must
go via our proxy. Instead run:

    ./scripts/update-docker-image.sh <IMAGE_NAME e.g. cohortextractor>

This will pull the updated image via the proxy, re-tag it as appopriate
and prune any unused images.


### Updating job-runner configuration

All configuration is via environment variables set in the `.env`
file.

For instance, to enable DEBUG level logging add the following line to
the `.env` file:

    LOGLEVEL=DEBUG

And then restart the service.


## Less common tasks


### A researcher says a job is "stuck", what should I do?

#### 1. Check it's not a problem with job-runner

The first thing is to confirm that this isn't a problem with job-runner
generally, rather than anything specific with this job.

On the jobs website find the page associated with the job which will
look something like: https://jobs.opensafely.org/jobs/dh6m4ocmvdzzkpgq/

Look for the field labelled "Last Updated by Runner". This tells you
when the job-runner last "checked in" with the job. Usually this is
under a minute ago. If the timestamp is recent then there's not an issue
with job-runner and you can proceed to step 2.

If it's longer than a few minutes then it could just be that job-runner
is busy copying some large files around. For simplicity's sake
job-runner is single threaded and so if it's busy copying files for
another job then it won't be checking in with this job. Check the end of
the log file to confirm this:

    tail -f service.err.log

If it looks like the job-runner is mid way through copying files then
wait for it to finish and confirm that the "Last Updated by Runner"
timestamp gets updated.

If job-runner isn't busy copying files and the "Last Updated by Runner"
timestamp still isn't updated then something has gone wrong. It's worth
trying a restart of the job-runner but if that doesn't unstick it then
you'll need to escalate.


#### 2. Check the individual job log

Everything a job writes to stdout or stderr ends up in its log file.
You can view these logs while the job is still running using the
command:

    ./scripts/watch-job-logs.sh -t

This will list all running jobs and allow you to select one. The `-t`
flag includes timestamps which will be useful here.

If you know the job's ID you can supply part of it to the command to
filter the list of jobs. Just a few characters is generally enough to be
unique e.g. for the example above you could do

    ./scripts/watch-job-logs.sh dh6m -t

The last line of log output might tell you somethig useful about what
the job has got stuck doing, or it might not; it all depends on what
sort of logging the researcher has added.


#### 3. Mount the job's volume and inspect its outputs

Each job has a Docker volume which it mounts to `/workspace` and is
where all its code, input files and output files are stored.

You can mount the volume of a running job and poke around it using the
command:

    ./scripts/mount-job-volume.sh

Again, for convenience this accepts partial job IDs e.g.

    ./scripts/mount-job-volume.sh dh6m

Inspecting the `/workspace` directory will tell you what outputs the job
has already produced which may be useful in determining what it has done
and where it has got stuck. If the job is doing additional logging to
explicitly named log files (rather than just to stdout) then mounting
will also allow you to access them.


#### 4. Strace the job and see what it's doing

The container you end up in via the `mount-job-volume.sh` command is a
privileged container which can access the PID namespace of running jobs.
This means you can run debugging tools against the running process.

First, determine the command being run by the job. You can do this by
running (in the git-bash shell **not** inside the container):

    docker ps --no-trunc --format '{{.Names}} {{.Command}}' | grep <PART_OF_JOB_ID>

Then mount the volume:

    ./scripts/mount-job-volume.sh <PART_OF_JOB_ID>

And then search for the relevant command in the process tree:

    ps faux | less

This should enable you to find the PID of the process (first numeric
column).

Once you have this you can run strace against the process:

    strace -fyp <PID>

If strace gives you no output then this suggests the job is doing
something CPU intensive. At this point you'll have to discuss with the
researcher what they think their job might be doing.



### Retrying a job which failed with "Internal error"

When a job fails with the message "Internal error" this means that
something unexpected happened and an exception other than JobError was
raised. This can be a bug in our code, or something unexpected in the
environment. (Windows has sometimes given us an "I/O Error" on
perfectly normal file operations.)

When this happens the job's container and volume are not
automatically cleaned up and so it's possible to retry the job without
having to start from scratch. If the bug has been fixed or you have some
other reason to think it was transient then you can re-try the job with:

    bash scripts/run.sh -m jobrunner.cli.retry_job <job_id>

The `job_id` actually only has to be a sub-string of the job ID (full
ones are a bit awkward to type) and you will be able to select the
correct job if there are multiple matches.

If you're not going to re-run the job and there's no further debugging
to be done then it's a good idea to clean up the job (see below).


### Killing a job

To kill a running job (or prevent it starting if it hasn't yet) use the
`kill_job` command:

    bash scripts/run.sh -m jobrunner.cli.kill_job --cleanup <job_id> [... <job_id>]

The `job_id` actually only has to be a sub-string of the job ID (full
ones are a bit awkward to type) and you wil be able to select the
correct job if there are multiple matches.

Multiple job IDs can be supplied to kill multiple jobs simultaneously.

The `--cleanup` flag deletes any associated containers and volumes,
which is generally what you want.

If you want to kill a job but leave the container and volume in place
for debugging then omit this flag.

It's fine to run this command against a job that's already been killed
so you can use it with the `--cleanup` flag to remove containers and
volumes of jobs that have already finished.


### Preparing for reboot

Sometimes we need to restart Docker, or reboot the VM in which we're
running, or reboot the entire host machine. When the happens, it's nicer
if we can automatically restart any running jobs rather than have them
fail and force the user to manually restart them.

To do this, first stop the job-runner service (see above).

After the service is stopped you can run the `prepare_for_reboot` command:

    bash scripts/run.sh -m jobrunner.cli.prepare_for_reboot

This is quite a destructive command as it will destroy the containers
and volumes for any running jobs. It will also reset any currently
running jobs to the pending state.

The next time job-runner restarts (which should be after the reboot) it
will pick up these jobs again as if it had not run them before and the
user should not have to do anything.


### Clearing up diskspace

To view current disk usage:

    docker system df -v

Generally, only running containers are doing anything useful. Stopped
containers are usually safe to delete.  To clean up stopped containers:

    docker containers prune

Typically, it will be orphaned volumes that will be taking up space. To delete
orphaned volumes that have no associated container:

    docker volume prune
