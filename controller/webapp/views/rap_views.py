import logging

from django.http import JsonResponse
from django.views.decorators.http import require_GET

from controller.webapp.views.auth import require_backend_authentication
from controller.webapp.views.tracing import trace_attributes


log = logging.getLogger(__name__)


@require_GET
@require_backend_authentication
def backend_status(request, backend):
    trace_attributes(backend=backend)
    # placeholder to return backend flags
    return JsonResponse({"flags": ""})
