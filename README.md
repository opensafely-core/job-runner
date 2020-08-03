A runner which encapsulates the task of checking out an OpenSAFELY
study repo and storing its results in a particular location.

It also has a `watch` mode where it polls a REST API for jobs to
execute, and posts the result there.

Each instance of a runner is expected to consume jobs for just one
backend (e.g. `tpp`); this is one of the required environment variables.

To run a server in watch mode, copy `dotenv-sample` to `.env` and edit its values; then

    docker-compose up

To run a single job, run:

    docker-compose job-runner run https://github.com/opensafely/somerepo sometag

The image is published automatically to Github Packages whenever
branches are merged.  To build the image locally:

    docker-compose build --build-arg pythonversion=3.8.1

To run without docker, set environment variables per the `environment` key in
`docker-compose.yml`, and run:

    python run.py watch https://jobs.opensafely.org/jobs

There are [integration tests in a separate repo](https://github.com/opensafely/job-integration-tests) to check interaction between the job server and the job runner
