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
    def from_request(cls, body_data: dict): ...

    @staticmethod
    def validate_schema(body_data, schema_name):
        schema = api_spec_json["components"]["schemas"][schema_name]
        try:
            jsonschema.validate(instance=body_data, schema=schema)
        except jsonschema.exceptions.ValidationError as err:
            if err.json_path == "$":
                error_message = f"Invalid request body received: {err.message}"
            else:
                error_message = (
                    f"Invalid request body received at {err.json_path}: {err.message}"
                )
            log.error(error_message)
            raise APIValidationError(error_message)


@dataclass
class CancelRequest(RequestBody):
    """
    Represents a request to cancel one or more actions associated with
    a single rap_id
    """

    rap_id: str
    actions: list[str]

    @classmethod
    def from_request(cls, body_data: dict):
        cls.validate_schema(body_data, "cancelRequestBody")

        rap_id = body_data["rap_id"]
        actions = body_data["actions"]

        return cls(
            rap_id=rap_id,
            actions=actions,
        )


@dataclass
class CreateRequest(RequestBody):
    """
    Represents a request to create a RAP

    Note: a RAP is currently still referred to as a `job_request`
    in other parts of the code
    """

    id: str
    backend: str
    workspace: str
    repo_url: str
    branch: str
    commit: str
    database_name: str
    requested_actions: list[str]
    codelists_ok: bool
    force_run_dependencies: bool
    created_by: str
    project: str
    orgs: list[str]
    analysis_scope: dict
    original: dict

    @classmethod
    def from_request(cls, body_data: dict):
        cls.validate_schema(body_data, "createRequestBody")

        workspace_name = body_data["workspace"]
        branch = body_data["branch"]
        # Construct the "original" dict with the workspace construction we need in
        # controller.main.job_to_job_definition()
        original = {
            **body_data,
            "workspace": {"name": workspace_name, "branch": branch},
        }

        return cls(
            id=body_data["rap_id"],
            backend=body_data["backend"],
            workspace=workspace_name,
            repo_url=body_data["repo_url"],
            branch=branch,
            commit=body_data["commit"],
            database_name=body_data["database_name"],
            requested_actions=body_data["requested_actions"],
            codelists_ok=body_data["codelists_ok"],
            force_run_dependencies=body_data["force_run_dependencies"],
            created_by=body_data["created_by"],
            project=body_data["project"],
            orgs=body_data["orgs"],
            analysis_scope=body_data.get("analysis_scope") or {},
            original=original,
        )

    def get_tracing_span_attributes(self) -> dict:
        """Provide useful attributes for telemetry suitable for passing
        as the `attributes` parameter to `start_as_current_span`."""
        return {
            "backend": self.backend,
            "workspace": self.workspace,
            "user": self.created_by,
            "project": self.project,
            "orgs": self.orgs,
        }


@dataclass
class StatusRequest(RequestBody):
    """
    Represents a request for the status of the RAPs corresponding to the given list of
    rap_ids.
    """

    rap_ids: list[str]

    @classmethod
    def from_request(cls, body_data: dict):
        cls.validate_schema(body_data, "statusRequestBody")

        rap_ids = body_data["rap_ids"]

        return cls(
            rap_ids=rap_ids,
        )
