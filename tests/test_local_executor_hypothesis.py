from hypothesis.stateful import (  # precondition,
    RuleBasedStateMachine,
    initialize,
    invariant,
    rule,
)


# from pytest import MonkeyPatch


class LocalExecutorMachine(RuleBasedStateMachine):
    @initialize()
    def setup(self):
        ...

    @rule()
    def my_rule(self):
        ...

    @invariant()
    def invar(self):
        ...

    def teardown(self):
        ...


TestLocalExecutor = LocalExecutorMachine.TestCase
