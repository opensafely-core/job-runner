import importlib

from jobrunner import config
from jobrunner.job_executor import NullWorkspaceAPI


def get_job_api():
    module_name, cls = config.EXECUTOR.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, cls)()


def get_workspace_api():
    return NullWorkspaceAPI()
