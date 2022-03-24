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
        self._state_cache = LRUDict(100)  # Maps job ids to states
        self._wrapped = wrapped
        self._add_logging(self._wrapped.get_status)
        self._add_logging(self._wrapped.prepare)
        self._add_logging(self._wrapped.execute)
        self._add_logging(self._wrapped.finalize)
        self._add_logging(self._wrapped.terminate)
        self._add_logging(self._wrapped.cleanup)

    def get_results(self, job: JobDefinition) -> JobResults:
        return self._wrapped.get_results(job)

    def delete_files(self, workspace: str, privacy: Privacy, paths: [str]) -> List[str]:
        return self._wrapped.delete_files(workspace, privacy, paths)

    def _add_logging(self, method: Callable[[JobDefinition], JobStatus]):
        def wrapper(job: JobDefinition) -> JobStatus:
            status = method(job)
            if self._is_new_state(job, status.state):
                self._write_log(job, status)
                self._state_cache[job.id] = status.state
            return status

        setattr(self, method.__name__, wrapper)

    def _is_new_state(self, job, state):
        return job.id not in self._state_cache or self._state_cache[job.id] != state

    def _write_log(self, job, status):
        log = f"State change for job {job.id}: {self._state_cache.get(job.id)} -> {status.state}"
        if status.message:
            log += f" ({status.message})"
        self._logger.info(log)
