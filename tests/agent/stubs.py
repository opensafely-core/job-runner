import time
from collections import defaultdict

from jobrunner.job_executor import ExecutorState, JobDefinition, JobStatus
from jobrunner.schema import AgentTask
from tests.factories import (
    canceljob_db_task_factory,
    job_factory,
    runjob_db_task_factory,
)


class StubExecutorAPI:
    """Dummy implementation of the ExecutorAPI, for use in tests.

    It tracks the current state of any jobs based the calls to the various API
    methods, and get_status() will return the current JobStatus.

    You can inject new job tasks to the executor with add_test_runjob_task()
    and add_test_canceljob_task(), for which you must supply a current ExecutorState.

    By default, transition methods successfully move to the next state. If you
    want to change that, call set_job_transition(job, state), and the next
    transition method call for that job will instead return that state. Optionally
    also provide a timestamp_ns to specify the timestamp of the next state transition.

    It also tracks which methods were called with which job ids to check the
    correct series of methods was invoked.

    """

    def __init__(self):
        self.tracker = {
            "prepare": set(),
            "execute": set(),
            "finalize": set(),
            "terminate": set(),
            "cleanup": set(),
        }
        self.transitions = {}
        self.metadata = {}
        self.job_statuses = {}
        self.deleted = defaultdict(lambda: defaultdict(list))
        self.last_time = int(time.time())

    def add_test_runjob_task(
        self,
        executor_state,
        timestamp_ns=None,
        **kwargs,
    ) -> AgentTask:
        """Create and track a db job object."""
        job = job_factory(**kwargs)
        task = AgentTask.from_task(runjob_db_task_factory(job=job))
        job = JobDefinition.from_dict(task.definition)
        self.set_job_status(job.id, executor_state, timestamp_ns)
        return task, job.id

    def add_test_canceljob_task(
        self,
        executor_state,
        timestamp_ns=None,
        **kwargs,
    ) -> AgentTask:
        job = job_factory(**kwargs)
        task = AgentTask.from_task(canceljob_db_task_factory(job=job))
        job = JobDefinition.from_dict(task.definition)
        self.set_job_status(job.id, executor_state, timestamp_ns)
        return task, job.id

    def set_job_status(self, job_id, executor_state, timestamp_ns=None):
        """Directly set a job status from an ExecutorState."""
        # handle the synchronous state meaning the state has completed
        if timestamp_ns is None:
            timestamp_ns = time.time_ns()
        status = JobStatus(state=executor_state, timestamp_ns=timestamp_ns)
        status.results = self.metadata.get(job_id, {})
        self.job_statuses[job_id] = status

    def set_job_transition(
        self, job_id, next_executor_state, timestamp_ns=None, hook=None
    ):
        """Set the next transition for this job when called"""
        self.transitions[job_id] = (next_executor_state, timestamp_ns, hook)

    def set_job_metadata(self, job_id, **kwargs):
        defaults = {
            "outputs": {},
            "unmatched_patterns": [],
            "unmatched_outputs": [],
            "exit_code": 0,
            "docker_image_id": "image_id",
            "status_message": "message",
            "action_version": "unknown",
            "action_revision": "unknown",
            "action_created": "unknown",
            "base_revision": "unknown",
            "base_created": "unknown",
            "cancelled": False,
        }
        kwargs = {**defaults, **kwargs}
        self.metadata[job_id] = kwargs

    def do_transition(
        self,
        job,
        expected_executor_state,
        next_executor_state,
        transition="",
    ):
        timestamp_ns = None
        current_job_status = self.get_status(job)
        if current_job_status.state != expected_executor_state:
            executor_state = current_job_status.state
        elif job.id in self.transitions:
            executor_state, timestamp_ns, hook = self.transitions.pop(job.id)
            if hook:
                hook(job)
        else:
            executor_state = next_executor_state

        timestamp_ns = timestamp_ns or time.time_ns()
        self.set_job_status(job.id, executor_state, timestamp_ns)

    def prepare(self, job):
        self.tracker["prepare"].add(job.id)
        self.do_transition(
            job, ExecutorState.UNKNOWN, ExecutorState.PREPARED, "prepare"
        )

    def execute(self, job):
        self.tracker["execute"].add(job.id)
        self.do_transition(
            job, ExecutorState.PREPARED, ExecutorState.EXECUTING, "execute"
        )

    def finalize(self, job, cancelled=False, error=None):
        if cancelled or error:
            # a finalize can be called from any status if we're cancelling or erroring
            executor_state = self.get_status(job).state
        else:
            executor_state = ExecutorState.EXECUTED
        self.tracker["finalize"].add(job.id)

        if error:
            # ensure passed error data is set in metadata
            metadata = self.metadata.get(job.id, {})
            metadata["error"] = error
            self.set_job_metadata(job.id, **metadata)

        self.do_transition(
            job,
            executor_state,
            ExecutorState.ERROR if error else ExecutorState.FINALIZED,
            "finalize",
        )

    def terminate(self, job):
        self.tracker["terminate"].add(job.id)
        if self.get_status(job).state == ExecutorState.UNKNOWN:
            # job was cancelled before it started running
            self.do_transition(
                job,
                ExecutorState.UNKNOWN,
                ExecutorState.UNKNOWN,
                "terminate",
            )
        elif self.get_status(job).state == ExecutorState.PREPARED:
            # job was cancelled after it was prepared, but before it started running
            # We do not need to terminate, so proceed directly to FINALIZED
            self.do_transition(
                job,
                ExecutorState.PREPARED,
                ExecutorState.FINALIZED,
                "terminate",
            )
        else:
            # job was cancelled after it started running
            self.do_transition(
                job,
                ExecutorState.EXECUTING,
                ExecutorState.EXECUTED,
                "terminate",
            )

    def cleanup(self, job):
        self.tracker["cleanup"].add(job.id)
        self.job_statuses.pop(job.id, None)
        # TODO: this currently does a silent error in some tests, if the initial
        # ExecutorState is not ERROR
        self.do_transition(job, ExecutorState.ERROR, ExecutorState.UNKNOWN, "cleanup")

    def get_status(self, job, cancelled=False):
        return self.job_statuses.get(job.id, JobStatus(ExecutorState.UNKNOWN))

    def delete_files(self, workspace, privacy, files):
        self.deleted[workspace][privacy].extend(files)
