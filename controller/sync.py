# TODO: Move to job_request.py

from controller.models import JobRequest


def job_request_from_remote_format(job_request):
    """
    Convert a JobRequest as received from the job-server into our own internal
    representation
    """
    return JobRequest(
        id=str(job_request["identifier"]),
        repo_url=job_request["workspace"]["repo"],
        commit=job_request["sha"],
        branch=job_request["workspace"]["branch"],
        requested_actions=job_request["requested_actions"],
        cancelled_actions=job_request["cancelled_actions"],
        workspace=job_request["workspace"]["name"],
        codelists_ok=job_request["codelists_ok"],
        database_name=job_request["database_name"],
        force_run_dependencies=job_request["force_run_dependencies"],
        backend=job_request["backend"],
        original=job_request,
    )
