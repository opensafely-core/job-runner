set dotenv-load := true
set positional-arguments := true

# List available commands
default:
    @{{ just_executable() }} --list --unsorted

# Create a valid .env if none exists
_dotenv:
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ ! -f .env ]]; then
      echo "No '.env' file found; creating a default '.env' from 'dotenv-sample'"
      cp dotenv-sample .env
      ./local-setup.sh
    fi

# Check if a .env exists
_checkenv:
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ ! -f .env ]]; then
        echo "No '.env' file found; run 'just devenv' to create one"
        exit 1
    fi

# Clean up temporary files
clean:
    rm -rf .venv

# Install production requirements into and remove extraneous packages from venv
prodenv:
    uv sync --no-dev

# && dependencies are run after the recipe has run. Needs just>=0.9.9. This is
# a killer feature over Makefiles.
#

# Install dev requirements into venv without removing extraneous packages
devenv: _dotenv && install-precommit
    uv sync --inexact

# Ensure precommit is installed
install-precommit:
    #!/usr/bin/env bash
    set -euo pipefail

    BASE_DIR=$(git rev-parse --show-toplevel)
    test -f $BASE_DIR/.git/hooks/pre-commit || uv run pre-commit install

# Upgrade a single package to the latest version as of the cutoff in pyproject.toml
upgrade-package package: && uvmirror devenv
    uv lock --upgrade-package {{ package }}

# Upgrade all packages to the latest versions as of the cutoff in pyproject.toml
upgrade-all: && uvmirror devenv
    uv lock --upgrade

# update the companion requirements.txt formatted file
uvmirror file="requirements.uvmirror.txt":
    rm -f {{ file }}
    uv export --format requirements-txt --frozen --no-hashes --all-groups --all-extras > {{ file }}

# Move the cutoff date in pyproject.toml to N days ago (default: 7) at midnight UTC
bump-uv-cutoff days="7":
    #!/usr/bin/env -S uvx --with tomlkit python3.13
    # Note we specify the python version here and we don't care if it's different to
    # the .python-version; we need 3.11+ for the datetime code used.

    import datetime
    import tomlkit

    with open("pyproject.toml", "rb") as f:
        content = tomlkit.load(f)

    new_datetime = (
        datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=int("{{ days }}"))
    ).replace(hour=0, minute=0, second=0, microsecond=0)
    new_timestamp = new_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    if existing_timestamp := content["tool"]["uv"].get("exclude-newer"):
        if new_datetime < datetime.datetime.fromisoformat(existing_timestamp):
            print(
                f"Existing cutoff {existing_timestamp} is more recent than {new_timestamp}, not updating."
            )
            exit(0)
    content["tool"]["uv"]["exclude-newer"] = new_timestamp

    with open("pyproject.toml", "w") as f:
        tomlkit.dump(content, f)

# This is the default input command to update-dependencies action
# https://github.com/bennettoxford/update-dependencies-action

# Bump the timestamp cutoff to midnight UTC 7 days ago and upgrade all dependencies
update-dependencies: upgrade-pipeline bump-uv-cutoff upgrade-all

# Upgrade version of opensafely-pipeline library
upgrade-pipeline: && devenv
    ./scripts/upgrade-pipeline.sh pyproject.toml

# Run the tests
test *ARGS: _checkenv
    IMAGE_PULL_TIMEOUT=300 uv run coverage run --module pytest "$@"
    uv run coverage report || uv run coverage html

test-fast *ARGS: _checkenv
    uv run python -m pytest tests -m "not slow_test" "$@"

test-verbose *ARGS: _checkenv
    uv run python -m pytest tests/test_integration.py -o log_cli=true -o log_cli_level=INFO "$@"

test-no-docker *ARGS: _checkenv
    uv run python -m pytest -m "not needs_docker" "$@"

# Run an agent cli command locally
cli command *ARGS: _checkenv
    uv run python -m agent.cli.{{ command }} {{ ARGS }}

format *args:
    uv run ruff format --diff --quiet {{ args }} .

lint *args:
    uv run ruff check {{ args }} .

lint-actions:
    docker run --rm -v $(pwd):/repo:ro --workdir /repo rhysd/actionlint:1.7.8 -color

# Run the various dev checks but does not change any files
check:
    #!/usr/bin/env bash
    set -euo pipefail

    failed=0

    check() {
      echo -e "\e[1m=> ${1}\e[0m"
      rc=0
      # Run it
      eval $1 || rc=$?
      # Increment the counter on failure
      if [[ $rc != 0 ]]; then
        failed=$((failed + 1))
        # Add spacing to separate the error output from the next check
        echo -e "\n"
      fi
    }

    check "just check-lockfile"
    check "just format"
    check "just lint"
    check "just lint-actions"
    test -d docker/ && check "just docker/lint"

    if [[ $failed > 0 ]]; then
      echo -en "\e[1;31m"
      echo "   $failed checks failed"
      echo -e "\e[0m"
      exit 1
    fi

# validate uv.lock
check-lockfile:
    #!/usr/bin/env bash
    set -euo pipefail
    # Make sure dates in pyproject.toml and uv.lock are in sync
    unset UV_EXCLUDE_NEWER
    rc=0
    uv lock --check || rc=$?
    if test "$rc" != "0" ; then
        echo "Timestamp cutoffs in uv.lock must match those in pyproject.toml. See DEVELOPERS.md for details and hints." >&2
        exit $rc
    fi

# Fix any automatically fixable linting or formatting errors
fix:
    uv run ruff check --fix .
    uv run ruff format .
    just --fmt --unstable
    just --fmt --unstable --justfile docker/justfile

manage *args: _checkenv
    uv run python manage.py {{ args }}

add-job *args:
    just manage add_job {{ args }} --backend test

pause:
    just manage pause on test

unpause:
    just manage pause off test

prepare-for-reboot *args:
    just manage prepare_for_reboot {{ args }} --backend test

# Run db migrations locally
migrate:
    just manage migrate_controller

# Run the dev project
run-agent: _checkenv
    uv run python -m agent.main

run-controller: _checkenv
    uv run python -m controller.main

run-app: _checkenv
    just manage runserver 3000

run-agent-service: _checkenv
    uv run python -m agent.service

run-controller-service: _checkenv
    uv run python -m controller.service

_run-agent-after-app:
    #!/usr/bin/env bash
    while [[  $(curl -s -o /dev/null -w "%{http_code}" http://localhost:3000) == "000" ]]; do
        echo "Waiting for web app to start..."
        sleep 1
    done;
    just run-agent-service

# Run all services together
run:
    { just run-app & just _run-agent-after-app & just run-controller-service; }

_schemathesis *ARGS: _checkenv
    uv run schemathesis --config-file controller/webapp/api_spec/schemathesis.toml run controller/webapp/api_spec/openapi.yaml {{ ARGS }}

schemathesis *ARGS:
    just _schemathesis --url http://localhost:3000/controller/v1 {{ ARGS }}

test-api-spec *ARGS:
    #!/bin/bash
    # Run webapp only in container (publishing port 3030) and kill on exit
    trap 'cd docker && docker compose kill test-controller-web-dev' EXIT
    just docker/run-detached-webapp
    # Give it long enough to be able to contact the server; if we do this too quickly, we
    # get a ConnectionResetError
    sleep 1
    just _schemathesis --url http://localhost:3030/controller/v1 {{ ARGS }}

# Install the Node.js dependencies

# (only used for api docs generation, not required in production)
assets-install *args="":
    #!/usr/bin/env bash
    set -euo pipefail

    # exit if lock file has not changed since we installed them. -nt == "newer than",
    # but we negate with || to avoid error exit code
    test package-lock.json -nt node_modules/.written || exit 0

    npm ci {{ args }}
    touch node_modules/.written

generate-api-docs: assets-install
    npm run build-docs

check-api-docs: generate-api-docs
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ -z $(git status --porcelain ./controller/webapp/api_spec/api_docs.html) ]]
    then
      echo "Generated docs are current."
    else
      echo "Generated docs are out of date."
      exit 1
    fi
