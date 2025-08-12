import json

from django.http import JsonResponse

from controller.webapp.views.validators.dataclasses import RequestBody
from controller.webapp.views.validators.exceptions import APIValidationError


def validate_request_body(dataclass: RequestBody):
    """
    Decorator to validate request body against a given dataclass.

    Wraps a function that is also decorated with @get_backends_for_client_token
    and is called with positional args `request` and `token_backends`, where
    `token_backends` is a list of backends that this client token has access to.

    Note: order of decorators is important; `get_backends_for_client_token` must
    come before `validate_request_body` so that the `token_backends` argument has been
    populated.

    @require_POST
    @get_backends_for_client_token
    @validate_request_body(RequestBody)
    def my_view(request, *, token_backends, request_obj: RequestBody):
        ...

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
            # Ensure that the token backends have been added to the view's kwargs. This verifies
            # that any view has the appropriate decorators applied in the correct order.

            # If the token_backends kwarg is present, the token has been authenticated and is valid
            # for at least one backend.
            # The RequestBody dataclass or the view itself is responsible for checking that the
            # client has access to the appropriate backend for the requested operation as
            # necessary
            assert "token_backends" in kwargs, (
                "`token_backends` keyword argument not found; ensure that the @get_backends_for_client_token "
                "decorator is before the @validate_request_body on this function"
            )

            # Django's request.POST is only populated from form data (i.e. content type
            # application/x-www-form-urlencoded). If we post with content type
            # application/json, the data will only be in the request body. Since we're
            # only expecting these endpoints to be called with json data, we use the
            # request.body
            try:
                body_data = json.loads(request.body.decode())
            except json.JSONDecodeError:
                return JsonResponse(
                    {
                        "error": "Validation error",
                        "details": "could not parse JSON from request body",
                    },
                    status=400,
                )

            try:
                obj = dataclass.from_request(body_data)
            except APIValidationError as e:
                return JsonResponse(
                    {"error": "Validation error", "details": e.args[0]}, status=400
                )

            kwargs["request_obj"] = obj
            return func(*args, **kwargs)

        return wrapper

    return decorator
