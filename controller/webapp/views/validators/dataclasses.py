import abc
from dataclasses import dataclass

from django.http.request import QueryDict


@dataclass
class RequestBody(abc.ABC):
    """
    Base class for validating and converting a request body to a dataclass instance.

    Subclassess must implement `from_request` method.
    """

    @classmethod
    @abc.abstractmethod
    def from_request(cls, post_data: QueryDict): ...
