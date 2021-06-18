# Developer notes

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
