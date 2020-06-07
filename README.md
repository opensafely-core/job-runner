A runner which encapsulates the task of checking out an OpenSAFELY
study repo and storing its results in a particular location.

It also has a `watch` mode where it polls a REST API for jobs to
execute, and posts the result there.


    DATABASE_URL=<db_for_backend> QUEUE_USER=queue_user QUEUE_PASS=queue_pass python run.py watch https://jobs.opensafely.org/jobs/

