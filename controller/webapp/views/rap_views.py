import logging
from pathlib import Path

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from controller.create_or_update_jobs import (
    related_jobs_exist,
    set_cancelled_flag_for_actions,
)
from controller.lib.database import count_where, exists_where, find_where
from controller.main import get_task_for_job
from controller.models import Job
from controller.queries import get_current_flags
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
        "name": backend,
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
    if not exists_where(
        Job, job_request_id=request_obj.rap_id, backend__in=token_backends
    ):
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
        job_request_id=request_obj.rap_id,
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

    log.info(
        "Cancelling actions for job_request %s: %s",
        request_obj.rap_id,
        request_obj.actions,
    )

    set_cancelled_flag_for_actions(request_obj.rap_id, request_obj.actions)
    cancelled_count = len(request_obj.actions)
    return JsonResponse(
        {
            "success": "ok",
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
        return JsonResponse(
            {
                "error": "Not allowed",
                "details": f"Not allowed for backend '{request_obj.backend}'",
            },
            status=403,
        )

    # Check jobs for job request ID don't already exist
    # We don't raise an error status code here; instead we return a 200 to
    # tell the client that jobs for the rap_id it requested have already been created.
    # The request completed successfully but did not create any new jobs (new job creation
    # will return a 201 - see below)
    if related_jobs_exist(request_obj):
        job_count = count_where(Job, job_request_id=request_obj.id)

        return JsonResponse(
            {
                "result": "No change",
                "details": f"Jobs already created for rap_id '{request_obj.id}'",
                "rap_id": request_obj.id,
                "count": job_count,
            },
            status=200,
        )

    # TODO: Catch errors and return error response (don't create exception jobs as we expect job-server
    #       to use the error response to mark the job request as failed
    # TODO: validate_repo_and_commit (note that the rest of validate_job_request() in create_or_update_jobs
    #       should be covered by the jsonschema validation in CreateRequest
    # TODO: Do the rest of create_jobs
    # TODO: Return a count of jobs created?

    return JsonResponse(
        {
            "result": "Success",
            "details": f"Jobs created for rap_id '{request_obj.id}'",
            "rap_id": request_obj.id,
            "count": 0,
        },
        status=201,
    )


# Fork of controller.sync.job_to_remote_format()
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
        "rap_id": job.job_request_id,
        "backend": job.backend,
        "action": job.action,
        "run_command": job.run_command,
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

    jobs = find_where(
        Job, job_request_id__in=request_obj.rap_ids, backend__in=token_backends
    )
    valid_rap_ids = {job.job_request_id for job in jobs}
    unrecognised_rap_ids = set(request_obj.rap_ids) - valid_rap_ids
    jobs_data = [job_to_api_format(i) for i in jobs]

    return JsonResponse(
        {"jobs": jobs_data, "unrecognised_rap_ids": list(unrecognised_rap_ids)},
        status=200,
    )
