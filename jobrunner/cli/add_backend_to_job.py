from jobrunner import config
from jobrunner.lib.database import find_all, find_where, update
from jobrunner.models import Job, SavedJobRequest


def main():
    """
    Command to add missing backend attribute to jobs. We expect this to only
    run once per backend, prior to moving the controller out of the backend
    """
    jobs = find_all(Job)
    jobs_missing_backend = find_where(Job, backend=None)
    if len(jobs) > len(jobs_missing_backend):
        print("This command has already been run, please confirm you want to re-run:")
        confirm = input("\nY to continue, N to quit\n")
        if confirm.lower() != "y":
            return

    for job in jobs_missing_backend:
        job_requests = find_where(SavedJobRequest, id=job.job_request_id)
        if job.job_request_id is None or not job_requests:
            # Some very old jobs have no job_request_id; as we expect this command to
            # be run from within a backend, we can just assign it from the config variable
            backend = config.BACKEND
        else:
            assert len(job_requests) == 1
            backend = job_requests[0].original.get("backend", config.BACKEND)

        job.backend = backend
        update(job, exclude_fields=["cancelled"])

    print(f"{len(jobs_missing_backend)} jobs updated")


if __name__ == "__main__":
    main()
