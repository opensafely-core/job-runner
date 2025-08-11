import logging
from pathlib import Path

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from controller.create_or_update_jobs import set_cancelled_flag_for_actions
from controller.lib.database import exists_where, find_where
from controller.models import Job
from controller.queries import get_current_flags
from controller.webapp.api_spec.utils import api_spec_json
from controller.webapp.views.auth.rap import (
    get_backends_for_client_token,
)
from controller.webapp.views.validators.dataclasses import CancelRequest
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
def backends_status(request, *, backends):
    flags = {backend: flags_for_backend(backend) for backend in backends}
    return JsonResponse({"flags": flags}, json_dumps_params={"separators": (",", ":")})


def flags_for_backend(backend):
    return {
        f.id: {"v": f.value, "ts": f.timestamp_isoformat}
        for f in get_current_flags(backend=backend)
    }


@csrf_exempt
@require_POST
@get_backends_for_client_token
@validate_request_body(CancelRequest)
def cancel(request, *, backends, request_obj: CancelRequest):
    """
    Cancel jobs for one or more actions associated with a job_request_id.

    The request should provide data in the format:

        {
            "backend": "<backend_name>"
            "job_request_id": "<id>",
            "actions": ["action1", "action2", ...]
        }
    """
    # Ensure that jobs exist for all requested cancel actions
    # We don't care about the state of the job (i.e. if it's already been cancelled), only
    # that it exists at all
    if not exists_where(Job, job_request_id=request_obj.job_request_id):
        return JsonResponse(
            {
                "error": "job request not found",
                "details": f"No jobs found for job_request_id {request_obj.job_request_id}",
                "job_request_id": request_obj.job_request_id,
            },
            status=400,
        )

    jobs = find_where(
        Job, job_request_id=request_obj.job_request_id, action__in=request_obj.actions
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
                "job_request_id": request_obj.job_request_id,
                "not_found": list(not_found),
            },
            status=400,
        )
    log.info(
        "Cancelling actions for job_request %s: %s",
        request_obj.job_request_id,
        request_obj.actions,
    )

    set_cancelled_flag_for_actions(request_obj.job_request_id, request_obj.actions)
    cancelled_count = len(request_obj.actions)
    return JsonResponse(
        {
            "success": "ok",
            "details": f"{len(request_obj.actions)} actions cancelled",
            "count": cancelled_count,
        },
        status=200,
    )
