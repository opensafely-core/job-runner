import logging
from typing import Callable, List

from jobrunner.job_executor import (
    ExecutorAPI,
    JobDefinition,
    JobResults,
    JobStatus,
    Privacy,
)
from jobrunner.lib.lru_dict import LRUDict


LOGGER_NAME = "executor"


class LoggingExecutor(ExecutorAPI):
    def __init__(self, wrapped: ExecutorAPI):
        self._logger = logging.getLogger(LOGGER_NAME)
        self._state_cache = LRUDict(1024)  # Maps job ids to states
        self._wrapped = wrapped
        self._add_logging(self._wrapped.get_status)
        self._add_logging(self._wrapped.prepare)
        self._add_logging(self._wrapped.execute)
        self._add_logging(self._wrapped.finalize)
        self._add_logging(self._wrapped.terminate)
        self._add_logging(self._wrapped.cleanup)

    def get_results(self, job_definition: JobDefinition) -> JobResults:
        return self._wrapped.get_results(job_definition)

    def delete_files(self, workspace: str, privacy: Privacy, paths: [str]) -> List[str]:
        return self._wrapped.delete_files(workspace, privacy, paths)

    @property
    def synchronous_transitions(self):
        return getattr(self._wrapped, "synchronous_transitions", [])

    def _add_logging(self, method: Callable[[JobDefinition], JobStatus]):
        def wrapper(job_definition: JobDefinition) -> JobStatus:
            status = method(job_definition)
            if self._is_new_state(job_definition, status.state):
                self._write_log(job_definition, status)
                self._state_cache[job_definition.id] = status.state
            return status

        setattr(self, method.__name__, wrapper)

    def _is_new_state(self, job_definition, state):
        return (
            job_definition.id not in self._state_cache
            or self._state_cache[job_definition.id] != state
        )

    def _write_log(self, job_definition, status):
        log = f"State change for job {job_definition.id}: {self._state_cache.get(job_definition.id)} -> {status.state}"
        if status.message:
            log += f" ({status.message})"
        self._logger.info(log)
