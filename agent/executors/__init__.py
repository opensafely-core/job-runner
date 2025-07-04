import importlib

from jobrunner.config import agent as config


def get_executor_api():
    module_name, cls = config.EXECUTOR.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, cls)()
