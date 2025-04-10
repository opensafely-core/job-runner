set dotenv-load := true
set positional-arguments := true

export VIRTUAL_ENV := env_var_or_default("VIRTUAL_ENV", ".venv")
export BIN := VIRTUAL_ENV + if os_family() == "unix" { "/bin" } else { "/Scripts" }
export PIP := BIN + if os_family() == "unix" { "/python -m pip" } else { "/python.exe -m pip" }

# List available commands
default:
    @{{ just_executable() }} --list --unsorted

# Ensure valid virtualenv
virtualenv:
    #!/usr/bin/env bash
    set -euo pipefail

    # allow users to specify python version in .env
    PYTHON_VERSION=${PYTHON_VERSION:-python3.10}

    # create venv and upgrade pip
    if [[ ! -d $VIRTUAL_ENV ]]; then
      $PYTHON_VERSION -m venv $VIRTUAL_ENV
      $PIP install --upgrade pip
    fi

# Run pip-compile with our standard settings
pip-compile *ARGS: devenv
    #!/usr/bin/env bash
    set -euo pipefail

    $BIN/pip-compile --allow-unsafe --generate-hashes --strip-extras "$@"

# Recompile production dependencies
pip-compile-prod *ARGS:
    just pip-compile "$@" pyproject.toml --output-file requirements.prod.txt

# Recompile development dependencies
pip-compile-dev *ARGS:
    just pip-compile "$@" requirements.dev.in --output-file requirements.dev.txt
    # Replace local absolute paths with their equivalent CI paths (just to avoid diff noise)
    sed -i -r "s#$PWD/#/home/runner/work/job-runner/job-runner/#" requirements.dev.txt

# Update all dependencies to latest version
update-dependencies:
    just pip-compile-prod --upgrade
    just pip-compile-dev --upgrade

# Create a valid .env if none exists
_dotenv:
    #!/usr/bin/env bash
    set -euo pipefail

    if [[ ! -f .env ]]; then
      echo "No '.env' file found; creating a default '.env' from 'dotenv-sample'"
      cp dotenv-sample .env
      ./local-setup.sh
    fi

# Ensure dev and prod requirements installed and up to date
devenv: virtualenv _dotenv
    #!/usr/bin/env bash
    set -euo pipefail

    for req_file in requirements.dev.txt requirements.prod.txt; do
      # If we've installed this file before and the original hasn't been
      # modified since then bail early
      record_file="$VIRTUAL_ENV/$req_file"
      if [[ -e "$record_file" && "$record_file" -nt "$req_file" ]]; then
        continue
      fi

      if cmp --silent "$req_file" "$record_file"; then
        # If the timestamp has been changed but not the contents (as can happen
        # when switching branches) then just update the timestamp
        touch "$record_file"
      else
        # Otherwise actually install the requirements

        # --no-deps is recommended when using hashes, and also works around a
        # bug with constraints and hashes. See:
        # https://pip.pypa.io/en/stable/topics/secure-installs/#do-not-use-setuptools-directly
        $PIP install --no-deps --requirement "$req_file"

        # Make a record of what we just installed
        cp "$req_file" "$record_file"
      fi
    done

    if [[ ! -f .git/hooks/pre-commit ]]; then
      $BIN/pre-commit install
    fi

# Upgrade version of opensafely-pipeline library
upgrade-pipeline: && pip-compile-prod
    ./scripts/upgrade-pipeline.sh pyproject.toml

# Run the tests
test *ARGS: devenv
    $BIN/coverage run --module pytest "$@"
    $BIN/coverage report || $BIN/coverage html

test-fast *ARGS: devenv
    $BIN/python -m pytest tests -m "not slow_test" "$@"

test-verbose *ARGS: devenv
    $BIN/python -m pytest tests/test_integration.py -o log_cli=true -o log_cli_level=INFO "$@"

test-no-docker *ARGS: devenv
    $BIN/python -m pytest -m "not needs_docker" "$@"

# Run db migrations locally
migrate:
    $BIN/python -m jobrunner.cli.migrate

# Lint and check formatting but don't modify anything
check: devenv
    #!/usr/bin/env bash

    failed=0

    check() {
      # Display the command we're going to run, in bold and with the "$BIN/"
      # prefix removed if present
      echo -e "\e[1m=> ${1#"$BIN/"}\e[0m"
      # Run it
      eval $1
      # Increment the counter on failure
      if [[ $? != 0 ]]; then
        failed=$((failed + 1))
        # Add spacing to separate the error output from the next check
        echo -e "\n"
      fi
    }

    check "$BIN/ruff format --diff --quiet ."
    check "$BIN/ruff check --output-format=full ."
    check "docker run --rm -i ghcr.io/hadolint/hadolint:v2.12.0-alpine < docker/Dockerfile"

    if [[ $failed > 0 ]]; then
      echo -en "\e[1;31m"
      echo "   $failed checks failed"
      echo -e "\e[0m"
      exit 1
    fi

# Fix any automatically fixable linting or formatting errors
fix: devenv
    $BIN/ruff format .
    $BIN/ruff check --fix .
    just --fmt --unstable --justfile justfile
    just --fmt --unstable --justfile docker/justfile

# Run the dev project
add-job *args: devenv
    $BIN/python -m jobrunner.cli.add_job {{ args }}

run-agent: devenv
    $BIN/python -m jobrunner.agent.main

run-controller: devenv
    $BIN/python -m jobrunner.controller.main
