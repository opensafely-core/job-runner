"""Custom exceptions to aid safe error reporting.

All exceptions should subclass OpenSafelyError

`safe_args` indicates if it is OK for any arguments to be sent to
people outside the secure platform.

"""


class OpenSafelyError(Exception):
    safe_args = False

    def __init__(self, *args, **kwargs):
        assert self.status_code not in [-1, 99], "status_codes -1 and 99 are reserved"
        super().__init__(*args, **kwargs)

    def safe_details(self):
        classname = type(self).__name__
        if self.safe_args:
            return classname + ": " + " ".join(self.args)
        else:
            return classname + ": [possibly-unsafe details redacted]"


class DockerError(OpenSafelyError):
    safe_args = False
    status_code = 1


class DockerRunError(DockerError):
    safe_args = False
    status_code = 3


class CohortExtractorError(DockerRunError):
    safe_args = False
    status_code = 4


class RepoNotFound(OpenSafelyError):
    safe_args = True
    status_code = 5


class InvalidRepo(OpenSafelyError):
    safe_args = True
    status_code = 6


class GitCloneError(OpenSafelyError):
    safe_args = True
    status_code = 7
