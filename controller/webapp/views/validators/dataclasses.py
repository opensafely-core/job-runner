import abc
import logging
from dataclasses import dataclass

import jsonschema

from controller.webapp.api_spec.utils import api_spec_json
from controller.webapp.views.validators.exceptions import APIValidationError


log = logging.getLogger(__name__)


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
    def from_request(cls, body_data: dict):
        schema = api_spec_json["components"]["schemas"]["cancelRequestBody"]
        try:
            jsonschema.validate(instance=body_data, schema=schema)
        except jsonschema.exceptions.ValidationError as err:
            log.error(err)
            raise APIValidationError(f"Invalid request body received: {err.message}")

        job_request_id = body_data["job_request_id"]
        actions = body_data["actions"]

        return cls(
            job_request_id=job_request_id,
            actions=actions,
        )
