import itertools
import logging
import time

import pytest

from common.lib.service_utils import ThreadWrapper


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_start_thread_returns(request, caplog):
    t = []

    def test_main():
        t.append(time.time())
        if len(t) < 3:
            return
        else:
            raise SystemExit  # bypass our exception handling and actually exit the thread

    interval = 0.1
    start_thread = ThreadWrapper(logging.getLogger())
    thread = start_thread(test_main, request.node.name, interval)
    thread.join()

    assert len(t) == 3
    for delta in (j - i for i, j in itertools.pairwise(t)):
        assert delta >= interval, (
            f"thread wrapper did not sleep between errors, expected more than {delta}"
        )
    # Only the main thread's startup message is logged
    assert len(caplog.records) == 1
    startup_log = caplog.records[0]
    assert startup_log.message == f"Starting {request.node.name} thread"
    assert startup_log.threadName == "MainThread"


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_start_thread_error(request, caplog):
    t = []

    def test_main():
        t.append(time.time())
        if len(t) < 3:
            raise Exception(f"thread exception {len(t)}")
        else:
            raise SystemExit  # bypass our exception handling and actually exit the thread

    interval = 0.1
    start_thread = ThreadWrapper(logging.getLogger())
    thread = start_thread(test_main, request.node.name, interval)
    thread.join()

    assert len(t) == 3
    for delta in (j - i for i, j in itertools.pairwise(t)):
        assert delta >= interval, (
            f"thread wrapper did not sleep between errors, expected more than {delta}"
        )

    assert len(caplog.records) == 3
    startup_log = caplog.records[0]
    assert startup_log.message == f"Starting {request.node.name} thread"
    assert startup_log.threadName == "MainThread"

    for i, r in enumerate(caplog.records[1:]):
        assert r.message == f"Exception in {request.node.name} thread"
        assert r.threadName == request.node.name
        assert str(r.exc_info[1]) == f"thread exception {i + 1}"


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_start_thread_error_quiet(request, caplog):
    class QuietException(Exception): ...

    t = []

    def test_main():
        # measure time called
        t.append(time.time())
        if len(t) < 3:
            raise QuietException(f"sync error {len(t)}")
        else:
            raise SystemExit  # bypass our exception handling and actually exit the thread

    interval = 0.1
    start_thread = ThreadWrapper(
        logging.getLogger(), quiet_exception_class=QuietException
    )
    thread = start_thread(test_main, request.node.name, interval)
    thread.join()

    assert len(t) == 3
    for delta in (j - i for i, j in itertools.pairwise(t)):
        assert delta >= interval, (
            f"thread wrapper did not sleep between errors, expected more than {delta}"
        )

    assert len(caplog.records) == 3
    startup_log = caplog.records[0]
    assert startup_log.message == f"Starting {request.node.name} thread"
    assert startup_log.threadName == "MainThread"

    for i, r in enumerate(caplog.records[1:]):
        assert r.message == f"sync error {i + 1}"
        assert r.threadName == request.node.name
        assert r.exc_info is None
