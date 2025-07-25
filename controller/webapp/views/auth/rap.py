from functools import wraps

from django.http import JsonResponse

from common import config as common_config
from controller import config


def require_client_token_backend_authentication(view_fn):
    """
    Ensure a valid client authentication token was received
    and nsure the token is valid for the specified backend
    """

    @wraps(view_fn)
    def wrapped_view(request, backend=None):
        if backend not in common_config.BACKENDS:
            return JsonResponse(
                {"error": "Not found", "details": f"Backend '{backend}' not found"},
                status=404,
            )
        token = request.headers.get("Authorization")
        error = None
        if not token:
            error = "No token provided"
        else:
            token_backends = config.CLIENT_TOKENS.get(token)
            if token_backends is None:
                error = "Invalid token"
            elif backend not in token_backends:
                error = f"Invalid token for backend '{backend}'"

        if error:
            return JsonResponse({"error": "Unauthorized", "details": error}, status=401)

        return view_fn(request, backend)

    return wrapped_view


def get_backends_for_client_token(view_fn):
    """
    Ensure a valid client authentication token was received
    and pass the allowed backends to the wrapped view function
    """

    @wraps(view_fn)
    def wrapped_view(request):
        token = request.headers.get("Authorization")
        error = None
        if not token:
            error = "No token provided"
        else:
            token_backends = config.CLIENT_TOKENS.get(token)
            if token_backends is None:
                error = "Invalid token"

        if error:
            return JsonResponse({"error": "Unauthorized", "details": error}, status=401)

        return view_fn(request, token_backends)

    return wrapped_view
