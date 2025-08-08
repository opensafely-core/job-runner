import json

from django.http import JsonResponse

from controller.webapp.views.validators.dataclasses import RequestBody
from controller.webapp.views.validators.exceptions import APIValidationError


def validator(dataclass: RequestBody):
    """
    Decorator to validate request body against a given dataclass.

    Args:
        dataclass: A dataclass, with a `from_request` method.

    Returns: either
        - the wrapped view, with an additional keyword argument
        `request_obj`, the dataclass instance created from the
        posted request.
        - JsonResponse with status 400
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            request = args[0]

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

            kwargs["request_obj"] = obj
            return func(*args, **kwargs)

        return wrapper

    return decorator
