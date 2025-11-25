import logging
from pathlib import Path

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST
from opentelemetry import trace
from pipeline import ProjectValidationError

from common.lib.git import GitError
from common.lib.github_validators import GithubValidationError
from common.tracing import duration_ms_as_span_attr, set_span_attributes
from controller.create_or_update_jobs import (
    NothingToDoError,
    RapCreateRequestError,
    create_jobs,
    related_jobs_exist,
    set_cancelled_flag_for_actions,
)
from controller.lib.database import exists_where, find_where, select_values
from controller.main import get_task_for_job
from controller.models import Job, State
from controller.queries import get_current_flags
from controller.reusable_actions import ReusableActionError
from controller.webapp.api_spec.utils import api_spec_json
from controller.webapp.views.auth.rap import (
    get_backends_for_client_token,
)
from controller.webapp.views.validators.dataclasses import (
    CancelRequest,
    CreateRequest,
    StatusRequest,
)
from controller.webapp.views.validators.decorators import validate_request_body


log = logging.getLogger(__name__)


def api_spec(request):
    return JsonResponse(api_spec_json)


def api_docs(request):
    return HttpResponse(
        (Path(__file__).parents[1] / "api_spec" / "api_docs.html").read_text()
    )


@csrf_exempt
@require_GET
@get_backends_for_client_token
def backends_status(request, *, token_backends):
    """
    Get status flags for all allowed backends.

    token_backends: a list of backends that the client token (provided in the
    request's Authorization header) has access to. Added by the
    get_backends_for_client_token decorator.
    """

    backends = [flags_for_backend(backend) for backend in token_backends]

    return JsonResponse(
        {"backends": backends}, json_dumps_params={"separators": (",", ":")}
    )


def flags_for_backend(backend):
    """
    Flags are arbitrary key/value pairs set for a backend, along with a timestamp. We are interested in a specific
    set of possible flags, which may or may not have ever been set:

        last-seen-at: set when the agent fetches tasks (in the /tasks endpoint)

        paused: set manually via manage command (webapp/management/commands/pause); possible values are "true" and None

        mode: set by the agent using the result of a DBSTATUS task OR set manually via manage command (webapp/management/commands/db_maintenance);
        possible values are "db-maintenance" and None

        manual-db-maintenance: manually via manage command (webapp/management/commands/db_maintenance);
        possible values are "on" and None. Always set in conjunction with setting mode (i.e. either mode="db-maintenance" AND manual-db-maintenance="on", or both are None)
    """

    # First define a dict of default values for a backend that has never had any of the flags of interest set
    flags_dict = {
        "slug": backend,
        "last_seen": None,
        "paused": {
            "status": "off",  # on/off
            "since": None,
        },
        "db_maintenance": {
            "status": "off",
            "since": None,
            "type": None,  # scheduled/manual/None
        },
    }

    flags = {f.id: f for f in get_current_flags(backend=backend)}

    if "last-seen-at" in flags:
        flags_dict["last_seen"] = flags["last-seen-at"].timestamp_isoformat
    if "paused" in flags:
        flags_dict["paused"]["since"] = flags["paused"].timestamp_isoformat
        if flags["paused"].value == "true":
            flags_dict["paused"]["status"] = "on"
    if "mode" in flags:
        flags_dict["db_maintenance"]["since"] = flags["mode"].timestamp_isoformat
        if flags["mode"].value == "db-maintenance":
            flags_dict["db_maintenance"]["status"] = "on"
            if (
                "manual-db-maintenance" in flags
                and flags["manual-db-maintenance"].value == "on"
            ):
                flags_dict["db_maintenance"]["type"] = "manual"
            else:
                flags_dict["db_maintenance"]["type"] = "scheduled"

    return flags_dict


@csrf_exempt
@require_POST
@get_backends_for_client_token
@validate_request_body(CancelRequest)
def cancel(request, *, token_backends, request_obj: CancelRequest):
    """
    Cancel jobs for one or more actions associated with a rap_id.

    token_backends: a list of backends that the client token (provided in the
    request's Authorization header) has access to. Added by the
    get_backends_for_client_token decorator.

    The request should provide data in the format:

        {
            "rap_id": "<id>",
            "actions": ["action1", "action2", ...]
        }
    """
    # Ensure that jobs exist for all requested cancel actions
    # We don't care about the state of the job (i.e. if it's already been cancelled), only
    # that it exists at all
    if not exists_where(Job, rap_id=request_obj.rap_id, backend__in=token_backends):
        return JsonResponse(
            {
                "error": "jobs not found",
                "details": f"No jobs found for rap_id {request_obj.rap_id}",
                "rap_id": request_obj.rap_id,
            },
            status=404,
        )

    jobs = find_where(
        Job,
        rap_id=request_obj.rap_id,
        action__in=request_obj.actions,
        backend__in=token_backends,
    )

    actions_to_cancel = {job.action for job in jobs}
    if not_found := set(request_obj.actions) - actions_to_cancel:
        not_found = sorted(not_found)
        not_found_actions = ",".join(not_found)
        log.error(
            "Jobs matching requested cancelled actions could not be found: %s",
            not_found_actions,
        )
        return JsonResponse(
            {
                "error": "jobs not found",
                "details": f"Jobs matching requested cancelled actions could not be found: {not_found_actions}",
                "rap_id": request_obj.rap_id,
                "not_found": list(not_found),
            },
            status=404,
        )

    num_actions_to_cancel = len(actions_to_cancel)
    if num_actions_to_cancel > 1:
        # Probably this is Job Server cancelling a whole request.
        # Let's avoid spamming the logs with all the actions in that case.
        log.info(
            "Cancelling %d actions for job_request %s",
            num_actions_to_cancel,
            request_obj.rap_id,
        )
    else:
        log.info(
            "Cancelling actions for job_request %s: %s",
            request_obj.rap_id,
            request_obj.actions,
        )

    set_cancelled_flag_for_actions(request_obj.rap_id, request_obj.actions)
    cancelled_count = len(request_obj.actions)
    return JsonResponse(
        {
            "result": "Success",
            "details": f"{len(request_obj.actions)} actions cancelled",
            "count": cancelled_count,
        },
        status=200,
    )


@csrf_exempt
@require_POST
@get_backends_for_client_token
@validate_request_body(CreateRequest)
def create(request, *, token_backends, request_obj: CreateRequest):
    """
    Create a new RAP (job request).

    token_backends: a list of backends that the client token (provided in the
    request's Authorization header) has access to. Added by the
    get_backends_for_client_token decorator.

    See controller/webapp/api_spec/openapi.yaml for required request body
    """
    if request_obj.backend not in token_backends:
        log.error(
            "Error creating jobs for rap_id %s: Not allowed for backend '%s'",
            request_obj.id,
            request_obj.backend,
        )
        return JsonResponse(
            {
                "error": "Error creating jobs",
                "details": "Unknown error",
            },
            status=400,
        )

    # Check jobs for job request ID don't already exist
    # We don't raise an error status code here; instead we return a 200 to
    # tell the client that jobs for the rap_id it requested have already been created.
    # The request completed successfully but did not create any new jobs (new job creation
    # will return a 201 - see below)
    if related_jobs_exist(request_obj):
        # We currently don't record any notion of which client requested the jobs to be created.
        # A rap_id must be unique, however, as it's provided by the client, there is the
        # possibility that if/when we have multiple clients, two client could make a request
        # with the same rap_id, but for different jobs.
        # This is just a crude check that the existing related jobs are consistent with this
        # request, by checking that the repo/commit/workspace (data which we have on the Job
        # model) match.

        related_jobs = find_where(Job, rap_id=request_obj.id)
        # All jobs for a single rap_id have the same repo_url, workspace and commit, so we
        # only need to check the first one
        job = related_jobs[0]
        job_data = {job.repo_url, job.workspace, job.commit}
        if job_data != {
            request_obj.repo_url,
            request_obj.workspace,
            request_obj.commit,
        }:
            log.error(
                (
                    "Received mismatched create request data for existing rap_id %s\n"
                    "repo_url: %s; received %s\n"
                    "commit: %s; received %s\n"
                    "workspace: %s; received %s"
                ),
                request_obj.id,
                job.repo_url,
                request_obj.repo_url,
                job.commit,
                request_obj.commit,
                job.workspace,
                request_obj.workspace,
            )
            return JsonResponse(
                {
                    "error": "Inconsistent request data",
                    "details": f"Jobs already created for rap_id '{request_obj.id}' are inconsistent with request data",
                },
                status=400,
            )

        log.info(f"Ignoring already processed rap_id:\n{request_obj.id}")

        return JsonResponse(
            {
                "result": "No change",
                "details": f"Jobs already created for rap_id '{request_obj.id}'",
                "rap_id": request_obj.id,
                "count": len(related_jobs),
            },
            status=200,
        )

    try:
        log.info(f"Handling new rap_id:\n{request_obj.id}")

        # TODO: create_jobs calls a method called validate_rap_create_request() which checks
        # various job request properties, and then calls validate_repo_and_commit
        # Everything other than validate_repo_and_commit should be covered by the jsonschema
        # validation in CreateRequest and so is not required now. It is left in for now
        # as technical debt from removing the controller sync loop.
        new_job_count = create_jobs(request_obj)
        log.info(f"Created {new_job_count} new jobs")

        return JsonResponse(
            {
                "result": "Success",
                "details": f"Jobs created for rap_id '{request_obj.id}'",
                "rap_id": request_obj.id,
                "count": new_job_count,
            },
            status=201,
        )
    except NothingToDoError as e:
        # No jobs have been created; this isn't really an error, and can occur due to:
        # - user requested run_all, and all actions have already run successfully
        # - pending or running jobs already exist for all the requested actions
        log.info("Nothing to do for rap_id %s:\n%s", request_obj.id, e)
        return JsonResponse(
            {
                "result": "Nothing to do",
                "details": str(e),
                "rap_id": request_obj.id,
                "count": 0,
            },
            status=200,
        )
    except (
        GitError,
        GithubValidationError,
        ProjectValidationError,
        ReusableActionError,
        RapCreateRequestError,
    ) as e:
        log.error("Failed to create jobs for rap_id %s:\n%s", request_obj.id, e)
        # Note: we return the error in the response for these specific handled errors, as
        # this is likely to contain useful information for the client to display to the
        # user regarding what went wrong. We control the content of these errors so it is
        # safe to pass them on in the response.
        return JsonResponse(
            {
                "error": "Error creating jobs",
                "details": str(e),
            },
            status=400,
        )

    except Exception:
        log.exception("Uncaught error while creating jobs")
        return JsonResponse(
            {
                "error": "Error creating jobs",
                "details": "Unknown error",
            },
            status=400,
        )


def job_to_api_format(job):
    """
    Convert our internal representation of a Job into the API format
    """

    metrics = {}
    if task := get_task_for_job(job):
        if task.agent_results:
            metrics = task.agent_results.get("job_metrics", {})

    return {
        "identifier": job.id,
        "rap_id": job.rap_id,
        "backend": job.backend,
        "action": job.action,
        "run_command": job.run_command or "",
        "status": job.state.value,
        "status_code": job.status_code.value,
        "status_message": job.status_message or "",
        "created_at": job.created_at_isoformat,
        "updated_at": job.updated_at_isoformat,
        "started_at": job.started_at_isoformat,
        "completed_at": job.completed_at_isoformat,
        "trace_context": job.trace_context,
        "metrics": metrics,
        "requires_db": job.requires_db,
    }


@csrf_exempt
@require_POST
@get_backends_for_client_token
@validate_request_body(StatusRequest)
def status(request, *, token_backends, request_obj: StatusRequest):
    """
    Get the status of an existing RAP (job request). Although this has no side-effects,
    use POST rather than GET in order to avoid any complications around request
    length etc.

    token_backends: a list of backends that the client token (provided in the
    request's Authorization header) has access to. Added by the
    get_backends_for_client_token decorator.

    If jobs exist for a given rap_id, but the client does not provide a valid
    token, it will return as unrecognised (rather than invalid token) in order
    to reduce information leakage.

    See controller/webapp/api_spec/openapi.yaml for required request body
    """
    # Add duration and attributes to the current django request span
    span = trace.get_current_span()
    with duration_ms_as_span_attr("find_matching_jobs.duration_ms", span):
        jobs = find_where(
            Job, rap_id__in=request_obj.rap_ids, backend__in=token_backends
        )
        valid_rap_ids = {job.rap_id for job in jobs}
        unrecognised_rap_ids = set(request_obj.rap_ids) - valid_rap_ids
        jobs_data = [job_to_api_format(i) for i in jobs]

    # Check for active jobs with RAP IDs that the client has NOT requested. We don't expect
    # this to happen, as jobs are only created at client request, and are only updated
    # on the client side via this endpoint. A job should never be marked as complete by the
    # client until after it has entered a complete state on the RAP controller.
    # NOTE: this only holds true as long as we have a single client.
    with duration_ms_as_span_attr("find_extra_rap_ids.duration_ms", span):
        extra_active_rap_ids = set(
            select_values(
                Job,
                "rap_id",
                state__in=[State.PENDING, State.RUNNING],
                backend__in=token_backends,
            )
        ) - set(request_obj.rap_ids)

    set_span_attributes(
        span,
        dict(
            valid_rap_ids=",".join(valid_rap_ids),
            unrecognised_rap_ids=",".join(unrecognised_rap_ids),
            extra_rap_ids=",".join(extra_active_rap_ids),
        ),
    )

    return JsonResponse(
        {"jobs": jobs_data, "unrecognised_rap_ids": list(unrecognised_rap_ids)},
        status=200,
    )
