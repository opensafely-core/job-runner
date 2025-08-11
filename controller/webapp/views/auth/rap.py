from functools import wraps

from django.http import JsonResponse

from controller import config


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
