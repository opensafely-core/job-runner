"""Custom exceptions to aid safe error reporting.

All exceptions should subclass OpenSafelyError

`report_args` indicates if it is OK for any arguments to be sent to
people outside the secure platform.

"""


class ErrorWithKeywordFactory:
    def __new__(self, *args):
        klass = args[0]
        report_args = args[1]
        args = args[1:]
        return klass(*args, report_args=report_args)


class OpenSafelyError(Exception):
    safe_args = False

    def __init__(self, *args, report_args=None):
        classname = type(self).__name__
        assert (
            report_args is not None
        ), f"`report_args` keyword arg must be supplied to {classname}"
        self.report_args = report_args
        assert self.status_code not in [-1, 99], "status_codes -1 and 99 are reserved"
        super().__init__(*args)

    def safe_details(self):
        classname = type(self).__name__
        if self.report_args:
            return classname + ": " + " ".join(self.args)
        else:
            return classname + ": [possibly-unsafe details redacted]"

    def __reduce__(self):
        """Override BaseException's custom pickling implementation, to support
        keyword arguments.
        """
        # The default implementation instantiates an instance with
        # just positional arguments, *then* updates its __dict__ with
        # keyword arguments.  This breaks the validation in this class'
        # __init__ function.
        #
        #
        # See https://stackoverflow.com/a/41809333/559140 for context
        return (
            ErrorWithKeywordFactory,
            (self.__class__, self.report_args) + self.args,
        )


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
