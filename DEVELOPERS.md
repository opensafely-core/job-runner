# Developer notes

## Dependencies

Install the development dependencies with:
```
pip install -r requirements.dev.txt
```
This includes the production dependencies in `requirements.txt` which
are intentionally kept minimal.

You will also need an up-to-date version of Docker Compose. Instructions to install it are [here](https://docs.docker.com/compose/install/).

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

### Testing in docker

To run tests in docker, simply run:

    make docker-test

This will build the docker image and run tests. You can run job-runner as
a service with:

    make docker-serve

Or run a command inside the docker image:

    make docker-run ARGS=command  # bash by default



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

Adding jobs locally is most easily done with the `jobrunner.cli.add_job`
command e.g
```
python -m jobrunner.cli.add_job https://github.com/opensafely/os-demo-research run_all
```

As well as URLs this will accept paths to local git repos e.g.
```
python -m jobrunner.cli.add_job ../os-demo-research run_all
```

If you now run the main loop you'll see it pick up the jobs:
```
python -m jobrunner.run
```

See the full set of options it accepts with:
```
python -m jobrunner.cli.add_job --help
```

## job-runner docker image

Building the dev docker image:

    make docker-build                   # build base and dev image
    make docker-build ENV=prod          # build base and prod image
    make docker-build ARGS=--no-cache   # build without cache


### Exposing the host's docker service

By default, running the docker container will mount your host's
`/var/run/docker.sock` into the container and use that for job-runner to run
jobs. It does some matching of docker GIDs to do so.

However, it also supports accessing docker over ssh:

    make -C docker enable-docker-over-ssh

The docker-compose invocations will now talk to your host docker over SSH,
possibly on a remote machine. You can disable with:

    make -C docker disable-docker-over-ssh

Note: The above commands will automatically generate a local ed25519
dev ssh key, and add it to your `~/.ssh/authorized_keys` file. You can use
`make docker-clean` to remove this.  If you wish to use a different user/host,
you can do so:

1. Specify `SSH_USER` and `SSH_HOST` environment variables.
2. Add an authorized ed25519 private key for that user to `docker/ssh/id_ed25519`.
3. Run `touch docker/ssh/id_ed25519.authorized` to let Make know that it is all
   set up.

