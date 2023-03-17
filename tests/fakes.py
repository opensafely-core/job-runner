from jobrunner.job_executor import ExecutorAPI, JobDefinition, JobStatus


class RecordingExecutor(ExecutorAPI):
    def __init__(self, *statuses):
        self.job_definition = None
        self._statuses = list(statuses)

    def _record(self, job_definition):
        self.job_definition = job_definition
        return self._statuses.pop(0)

    def get_status(self, job_definition: JobDefinition) -> JobStatus:
        return self._record(job_definition)

    def prepare(self, job_definition: JobDefinition) -> JobStatus:
        return self._record(job_definition)

    def execute(self, job_definition: JobDefinition) -> JobStatus:
        return self._record(job_definition)

    def finalize(self, job_definition: JobDefinition) -> JobStatus:
        return self._record(job_definition)

    def terminate(self, job_definition: JobDefinition) -> JobStatus:
        return self._record(job_definition)

    def cleanup(self, job_definition: JobDefinition) -> JobStatus:
        return self._record(job_definition)
