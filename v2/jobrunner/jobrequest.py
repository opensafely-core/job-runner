import uuid

from .database import transaction, insert, exists_where
from .git import read_file_from_repo, get_sha_from_remote_ref, GitError
from .project import parse_and_validate_project_file, ProjectValidationError


def create_or_update_jobs(job_request):
    if not related_jobs_exist(job_request):
        try:
            create_jobs(job_request)
        except (GitError, ProjectValidationError) as e:
            create_failed_job(job_request, e)
    else:
        # TODO: think about what sort of updates we want to support
        # I think these are probably limited to:
        #  * cancel any pending jobs
        #  * cancel any pending jobs and kill any running ones
        #  * update the target commit SHA for any pending jobs (although cancel and
        #    resubmit would also work for this and would probably be simpler)
        # update_jobs(job_request)
        pass


def related_jobs_exist(job_request):
    return exists_where("job", job_request_id=job_request["id"])


def create_jobs(job_request):
    if not job_request["commit"]:
        job_request["commit"] = get_sha_from_remote_ref(
            job_request["repo_url"], job_request["branch"]
        )

    project_file = read_file_from_repo(
        job_request["repo_url"], job_request["commit"], "project.yaml"
    )
    project = parse_and_validate_project_file(project_file)

    action = project["actions"][job_request["action"]]

    job = dict(
        id=str(uuid.uuid4()),
        job_request_id=job_request["id"],
        status="P",
        repo_url=job_request["repo_url"],
        commit=job_request["commit"],
        workspace=job_request["workspace"],
        action=job_request["action"],
        wait_for_job_ids_json=[],
        requires_outputs_from_json=action.get("needs", []),
        run_command=action["run"],
        output_spec_json=action["outputs"],
    )

    with transaction():
        insert(
            "job_request",
            dict(id=job_request["id"], original_json=job_request["original"]),
        )
        insert("job", job)


def create_failed_job(job_request, exception):
    with transaction():
        insert(
            "job_request",
            dict(id=job_request["id"], original_json=job_request["original"]),
        )
        insert(
            "job",
            dict(
                id=str(uuid.uuid4()),
                job_request_id=job_request["id"],
                status="F",
                workspace=job_request["workspace"],
                action=job_request["action"],
                error_message=f"{type(exception).__name__}: {exception}",
            ),
        )
