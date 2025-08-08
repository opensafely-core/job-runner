import json

from django.http import JsonResponse

from common import config
from controller.webapp.views.tracing import trace_attributes
from controller.webapp.views.validators.dataclasses import RequestBody
from controller.webapp.views.validators.exceptions import APIValidationError


def validator(dataclass: RequestBody):
    """
    Decorator to validate request body against a given dataclass.

    Wraps a function that is also decorated with @get_backends_for_client_token
    and is called with positional args `request` and `backends`, where `backends`
    is a list of backends that this client token has access to.

    Args:
        dataclass: A dataclass, with a `from_request` method.

    Returns: either
        - the wrapped view, with an additional keyword argument
        `request_obj`, the dataclass instance created from the
        posted request.
        - JsonResponse with status 400 (validation error)
          or 403 (no access to backend specified in post data)

    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            request = args[0]
            backends = args[1]

            # Django's request.POST is only populated from form data (i.e. content type
            # application/x-www-form-urlencoded). If we post with content type
            # application/json, the data will only be in the request body. Since we're
            # only expected these endpoints to be called with json data, we use the
            # request.body
            try:
                post_data = json.loads(request.body.decode())
            except json.JSONDecodeError:
                return JsonResponse(
                    {
                        "error": "Validation error",
                        "details": "could not parse JSON from request body",
                    },
                    status=400,
                )

            try:
                obj = dataclass.from_request(post_data)
            except APIValidationError as e:
                return JsonResponse(
                    {"error": "Validation error", "details": str(e)}, status=400
                )

            backend = post_data.get("backend")
            if backend not in config.BACKENDS:
                return JsonResponse(
                    {"error": "Not found", "details": f"Backend '{backend}' not found"},
                    status=404,
                )
            if backend not in backends:
                return JsonResponse(
                    {
                        "error": "Not allowed",
                        "details": f"Not allowed for backend '{backend}'",
                    },
                    status=403,
                )

            trace_attributes(backend=backend)

            kwargs["request_obj"] = obj
            return func(*args, **kwargs)

        return wrapper

    return decorator
