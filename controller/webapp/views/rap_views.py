import logging
from pathlib import Path

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET
from ruamel.yaml import YAML

from controller.queries import get_current_flags
from controller.webapp.views.auth.rap import (
    get_backends_for_client_token,
    require_client_token_backend_authentication,
)
from controller.webapp.views.tracing import trace_attributes


log = logging.getLogger(__name__)


def api_spec(request):
    yaml = YAML()
    return JsonResponse(
        yaml.load(Path(__file__).parents[1] / "api_spec" / "openapi.yaml")
    )


def api_docs(request):
    return HttpResponse(
        (Path(__file__).parents[1] / "api_spec" / "api_docs.html").read_text()
    )


@csrf_exempt
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
