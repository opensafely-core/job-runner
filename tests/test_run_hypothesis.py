import copy

from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)
from pytest import MonkeyPatch

from jobrunner import config, run
from jobrunner.job_executor import ExecutorState
from jobrunner.lib import database
from jobrunner.models import State, StatusCode
from tests.factories import StubExecutorAPI


class StubExecutorMachine(RuleBasedStateMachine):
    @initialize()
    def setup(self):
        # manually create the pytest fixtures we need

        """Create a throwaway db."""
        # name = request.node.name
        name = "tom123"
        self.database_file = f"file:db-{name}?mode=memory&cache=shared"
        monkeypatch = MonkeyPatch()
        monkeypatch.setattr(config, "DATABASE_FILE", self.database_file)
        database.ensure_db(self.database_file)

        # create the test objects
        self.api = StubExecutorAPI()
        self.job = self.api.add_test_job(
            ExecutorState.UNKNOWN, State.PENDING, StatusCode.CREATED
        )
        # self.volume_api = volumes.BindMountVolumeAPI
        # monkeypatch.setattr(volumes, "DEFAULT_VOLUME_API", request.param)
        # return request.param

    @rule()
    def cancel_job(self):
        self.job.cancelled = True
        # unclear if this should be here or not
        # the cancellation isn't triggered immediately, it happens at the next loop
        self.run_handle_job()

    @rule()
    def run_handle_job(self):
        # it's useful not to put this in a precondition, so that hypothesis never
        # runs out of things to run
        if self.job.state in [State.PENDING, State.RUNNING]:
            run.handle_job(self.job, self.api)

    @rule()
    @precondition(lambda self: self.job.state in [State.PENDING])
    def finish_preparing(self):
        self.api.set_job_status_from_executor_state(self.job, ExecutorState.PREPARED)
        # we need to run handle_job() before any invariants
        self.run_handle_job()

    @rule()
    @precondition(lambda self: self.job.state in [State.PENDING, State.RUNNING])
    def finish_executing(self):
        self.api.set_job_status_from_executor_state(self.job, ExecutorState.EXECUTED)
        # we need to run handle_job() before any invariants
        job_before = copy.deepcopy(self.job)
        self.run_handle_job()
        raise Exception(job_before, self.job)

    @rule()
    @precondition(
        lambda self: self.job.state in [State.RUNNING]
        and self.job.status_code not in [StatusCode.FINALIZED]
    )
    def finish_finalizing(self):
        self.api.set_job_status_from_executor_state(self.job, ExecutorState.FINALIZED)
        self.api.set_job_result(self.job)
        # we need to run handle_job() before any invariants
        self.run_handle_job()

    @invariant()
    def consistent_state_status_code(self):
        if self.job.state == State.PENDING:
            assert self.job.status_code in [StatusCode.CREATED]
        elif self.job.state == State.RUNNING:
            # raise Exception(self.job.state, self.job.status_code)
            assert self.job.status_code in [
                StatusCode.PREPARING,
                StatusCode.EXECUTING,
                StatusCode.FINALIZING,
            ]
        elif self.job.state == State.SUCCEEDED:
            # assert False
            assert self.job.status_code in [StatusCode.SUCCEEDED]
        elif self.job.state == State.FAILED:
            assert self.job.status_code in [StatusCode.CANCELLED_BY_USER]
        else:
            raise Exception(self.job.state)

    def teardown(self):
        del database.CONNECTION_CACHE.__dict__[self.database_file]


TestDontDie = StubExecutorMachine.TestCase
