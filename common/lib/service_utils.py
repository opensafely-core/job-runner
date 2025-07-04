import threading
import time


class ThreadWrapper:
    def __init__(self, log, quiet_exception_class=()):
        self.log = log
        self.quiet_exception_class = quiet_exception_class

    def __call__(self, func, name, loop_interval):
        """Start a thread running the given function.

        It is wrapped in function that will handle and log any exceptions.  This
        ensures any uncaught exceptions do not leave a zombie thread. We add
        a delay prevents busy retry loops.
        """

        def thread_wrapper():
            while True:
                try:
                    func()
                except Exception as e:
                    if isinstance(e, self.quiet_exception_class):
                        # We still want to log these, we just don't want the whole traceback
                        self.log.error(e)
                    else:
                        self.log.exception(f"Exception in {name} thread")

                time.sleep(loop_interval)

        self.log.info(f"Starting {name} thread")

        # daemon=True means this thread will be automatically join()ed when the
        # process exits
        thread = threading.Thread(target=thread_wrapper, daemon=True)
        thread.name = name
        thread.start()

        return thread
