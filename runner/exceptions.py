"""Custom exceptions to aid safe error reporting.

All exceptions should subclass OpenSafelyError

`report_args` indicates if it is OK for any arguments to be sent to
people outside the secure platform.

"""


class OpenSafelyError(Exception):
    safe_args = False

    def __init__(self, *args, report_args=None, **kwargs):
        assert report_args is not None, "`report_args` keyword arg must be supplied"
        self.report_args = report_args
        assert self.status_code not in [-1, 99], "status_codes -1 and 99 are reserved"
        super().__init__(*args, **kwargs)

    def safe_details(self):
        classname = type(self).__name__
        if self.report_args:
            return classname + ": " + " ".join(self.args)
        else:
            return classname + ": [possibly-unsafe details redacted]"


class DockerError(OpenSafelyError):
    status_code = 1


class DockerRunError(DockerError):
    status_code = 3


class CohortExtractorError(DockerRunError):
    status_code = 4


class RepoNotFound(OpenSafelyError):
    status_code = 5


class InvalidRepo(OpenSafelyError):
    status_code = 6


class GitCloneError(OpenSafelyError):
    status_code = 7
