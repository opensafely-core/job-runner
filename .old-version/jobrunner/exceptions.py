"""Custom exceptions to aid safe error reporting.

All exceptions should subclass OpenSafelyError

`report_args` indicates if it is OK for any arguments to be sent to
people outside the secure platform.

"""


class OpenSafelyError(Exception):
    safe_args = False

    def __init__(self, *args, report_args=False):
        self.report_args = report_args
        assert self.status_code not in [-1, 99], "status_codes -1 and 99 are reserved"
        super().__init__(*args)

    def safe_details(self):
        classname = type(self).__name__
        if self.report_args:
            return f"{classname}: {','.join(self.args)}"
        else:
            return classname + ": [possibly-unsafe details redacted]"


class DockerError(OpenSafelyError):
    status_code = 1


class DockerRunError(DockerError):
    status_code = 2


class RepoNotFound(OpenSafelyError):
    status_code = 3


class InvalidRepo(OpenSafelyError):
    status_code = 4


class GitCloneError(OpenSafelyError):
    status_code = 5


class DependencyNotFinished(OpenSafelyError):
    status_code = 6


class DependencyFailed(DependencyNotFinished):
    status_code = 7


class DependencyRunning(DependencyNotFinished):
    status_code = 8


class ProjectValidationError(OpenSafelyError):
    status_code = 9
