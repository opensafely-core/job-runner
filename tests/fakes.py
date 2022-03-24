from jobrunner.job_executor import ExecutorAPI, JobDefinition, JobStatus


class RecordingExecutor(ExecutorAPI):
    def __init__(self, *statuses):
        self.job = None
        self._statuses = list(statuses)

    def _record(self, job):
        self.job = job
        return self._statuses.pop(0)

    def get_status(self, job: JobDefinition) -> JobStatus:
        return self._record(job)

    def prepare(self, job: JobDefinition) -> JobStatus:
        return self._record(job)

    def execute(self, job: JobDefinition) -> JobStatus:
        return self._record(job)

    def finalize(self, job: JobDefinition) -> JobStatus:
        return self._record(job)

    def terminate(self, job: JobDefinition) -> JobStatus:
        return self._record(job)

    def cleanup(self, job: JobDefinition) -> JobStatus:
        return self._record(job)
