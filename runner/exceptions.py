class OpenSafelyError(Exception):
    pass


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
