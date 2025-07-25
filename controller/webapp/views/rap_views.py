import logging

from django.http import JsonResponse
from django.views.decorators.http import require_GET

from controller.queries import get_current_flags
from controller.webapp.views.auth.rap import (
    get_backends_for_client_token,
    require_client_token_backend_authentication,
)
from controller.webapp.views.tracing import trace_attributes


log = logging.getLogger(__name__)


@require_GET
@require_client_token_backend_authentication
def backend_status(request, backend):
    trace_attributes(backend=backend)
    flags = {
        f.id: {"v": f.value, "ts": f.timestamp_isoformat}
        for f in get_current_flags(backend=backend)
    }

    return JsonResponse({"flags": flags}, json_dumps_params={"separators": (",", ":")})


@require_GET
@get_backends_for_client_token
def backends_status(request, backends):
    flags = {backend: flags_for_backend(backend) for backend in backends}
    return JsonResponse({"flags": flags}, json_dumps_params={"separators": (",", ":")})


def flags_for_backend(backend):
    return {
        f.id: {"v": f.value, "ts": f.timestamp_isoformat}
        for f in get_current_flags(backend=backend)
    }
