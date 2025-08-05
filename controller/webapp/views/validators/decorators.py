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
        `request_body`, the dataclass instance created from the
        request POST data.
        - JsonResponse with status 400
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            request = args[0]
            try:
                obj = dataclass.from_request(request.POST)
            except APIValidationError as e:
                return JsonResponse(
                    {"error": "Validation error", "details": str(e)}, status=400
                )

            kwargs["request_obj"] = obj
            return func(*args, **kwargs)

        return wrapper

    return decorator
