#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 2 ]]; then
    echo "Usage: test-controller <backend> <token>" >&2
    exit 1
fi

backend=$1
token=$2

if [[ -z "$backend" || -z "$token" ]]; then
    echo "Error: backend and token must both be non-empty" >&2
    echo "Usage: test-controller <backend> <token>" >&2
    exit 1
fi

if [[ ! "$backend" =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "Error: backend must match ^[a-zA-Z0-9_]+$" >&2
    exit 1
fi

backend_var_prefix=$(echo "$backend" | tr '[:lower:]' '[:upper:]')
backend_token_var="${backend_var_prefix}_JOB_SERVER_TOKEN"
# Set backend-specific token variable dynamically e.g. TEST_JOB_SERVER_TOKEN
export "${backend_token_var}=${token}"

export BACKENDS="$backend"
export DJANGO_CONTROLLER_SECRET_KEY=$(python -c 'import secrets; print(secrets.token_urlsafe(32))')

export CONTROLLER_ENABLE_TICKS="False"
export DJANGO_CONTROLLER_ALLOWED_HOSTS="*"
export DJANGO_DEBUG="False"

/opt/venv/bin/python -m controller.cli.migrate

cat <<ENVVARS
Dummy controller started with:
BACKEND=${backend}
CONTROLLER_TASK_API_TOKEN=${token}
CONTROLLER_TASK_API_ENDPOINT=http://localhost:8000/
ENVVARS

# run the service in the background
run-one-constantly /opt/venv/bin/python -m controller.service &
controller_service_pid=$!

cleanup() {
    kill "$controller_service_pid" >/dev/null 2>&1 || true
    wait "$controller_service_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# run gunicorn in the foreground
exec /opt/venv/bin/gunicorn --bind 0.0.0.0:8000 --config gunicorn.conf.py controller.webapp.wsgi
