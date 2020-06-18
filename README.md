A runner which encapsulates the task of checking out an OpenSAFELY
study repo and storing its results in a particular location.

It also has a `watch` mode where it polls a REST API for jobs to
execute, and posts the result there.

To run in watch mode, copy `dotenv-sample` to `.env` and edit its values; then

    docker-compose up
