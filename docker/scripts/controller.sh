#!/bin/bash
set -euo pipefail

# Make sure the log output lines don't clobber each other
export PYTHONUNBUFFERED=True

# Run the control loop in the background (restarting if necessary)
# TODO: Replace with a Django management command for consistency
run-one-constantly /opt/venv/bin/python -m jobrunner.controller.service &

exec /opt/venv/bin/python manage.py runserver 0.0.0.0:8000
