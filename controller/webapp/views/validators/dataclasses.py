import abc
from dataclasses import dataclass

from controller.webapp.views.validators.exceptions import APIValidationError


@dataclass
class RequestBody(abc.ABC):
    """
    Base class for validating and converting a request body to a dataclass instance.

    Subclassess must implement `from_request` method.
    """

    @classmethod
    @abc.abstractmethod
    def from_request(cls, post_data: dict): ...


@dataclass
class CancelRequest(RequestBody):
    """
    Represents a request to cancel one or more actions associated with
    a single job_request
    """

    job_request_id: str
    actions: list[str]

    @classmethod
    def from_request(cls, post_data: dict):
        errors = []
        job_request_id = post_data.get("job_request_id")
        if not job_request_id:
            errors.append("job_request_id not provided")
        actions = post_data.get("actions")
        if not actions:
            errors.append("No actions provided")

        if errors:
            raise APIValidationError(", ".join(errors))
        return cls(
            job_request_id=job_request_id,
            actions=actions,
        )
