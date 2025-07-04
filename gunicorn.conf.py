import os

import jobrunner.tracing as tracing


# Where to log to (stdout and stderr)
accesslog = "-"
errorlog = "-"

# Configure log structure
# # http://docs.gunicorn.org/en/stable/settings.html#logconfig-dict
# logconfig_dict = logging_config_dict

# workers
workers = 5

# listen
bind = "0.0.0.0:8000"


# disable access logging, as not needed
accesslog = None


# Because of Gunicorn's pre-fork web server model, we need to initialise opentelemetry
# in gunicorn's post_fork method in order to instrument our application process, see:
# https://opentelemetry-python.readthedocs.io/en/latest/examples/fork-process-model/README.html
def post_fork(server, worker):
    # opentelemetry initialisation needs this, so ensure its set
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "controller.webapp.settings")
    server.log.info("Worker spawned (pid: %s)", worker.pid)
    tracing.setup_default_tracing()
