from functools import wraps

from django.http import JsonResponse

from common import config as common_config
from controller import config


def require_backend_authentication(view_fn):
    """Ensure a valid authentication token was received"""

    @wraps(view_fn)
    def wrapped_view(request, backend):
        if backend not in common_config.BACKENDS:
            return JsonResponse(
                {"error": "Not found", "details": f"Backend '{backend}' not found"},
                status=404,
            )
        token = request.headers.get("Authorization")
        error = None
        if not token:
            error = "No token provided"
        elif token != config.JOB_SERVER_TOKENS[backend]:
            error = f"Invalid token for backend '{backend}'"

        if error:
            return JsonResponse({"error": "Unauthorized", "details": error}, status=401)

        return view_fn(request, backend)

    return wrapped_view
