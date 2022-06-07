set dotenv-load := true

# just has no idiom for setting a default value for an environment variable
# so we shell out, as we need VIRTUAL_ENV in the justfile environment

export VIRTUAL_ENV := `echo ${VIRTUAL_ENV:-.venv}`
export BIN := VIRTUAL_ENV + if os_family() == "unix" { "/bin" } else { "/Scripts" }
export PIP := BIN + if os_family() == "unix" { "/python -m pip" } else { "/python.exe -m pip" }

# enforce our chosen pip compile flags

export COMPILE := BIN + "/pip-compile --allow-unsafe --generate-hashes"
export DEFAULT_PYTHON := if os_family() == "unix" { "python3.8" } else { "python" }

# list available commands
default:
    @"{{ just_executable() }}" --list

# clean up temporary files
clean:
    rm -rf .venv

# ensure valid virtualenv
virtualenv:
    #!/usr/bin/env bash
    # allow users to specify python version in .env
    PYTHON_VERSION=${PYTHON_VERSION:-$DEFAULT_PYTHON}

    # create venv and upgrade pip
    test -d $VIRTUAL_ENV || { $PYTHON_VERSION -m venv $VIRTUAL_ENV && $PIP install --upgrade pip; }

    # ensure we have pip-tools so we can run pip-compile
    test -e $BIN/pip-compile || $PIP install $(grep pip-tools== requirements.dev.txt)

_compile src dst *args: virtualenv
    #!/usr/bin/env bash
    # exit if src file is older than dst file (-nt = 'newer than', but we negate with || to avoid error exit code)
    test "${FORCE:-}" = "true" -o {{ src }} -nt {{ dst }} || exit 0
    $BIN/pip-compile --allow-unsafe --output-file={{ dst }} {{ src }} {{ args }}

# update requirements.prod.txt if setup.py has changed
requirements-prod *args:
    "{{ just_executable() }}" _compile pyproject.toml requirements.prod.txt {{ args }}

# update requirements.dev.txt if requirements.dev.in has changed
requirements-dev *args: requirements-prod
    "{{ just_executable() }}" _compile requirements.dev.in requirements.dev.txt {{ args }}

# ensure prod requirements installed and up to date
prodenv: requirements-prod
    #!/usr/bin/env bash
    # exit if .txt file has not changed since we installed them (-nt == "newer than', but we negate with || to avoid error exit code)
    test requirements.prod.txt -nt $VIRTUAL_ENV/.prod || exit 0

    $PIP install -r requirements.prod.txt
    touch $VIRTUAL_ENV/.prod

# && dependencies are run after the recipe has run. Needs just>=0.9.9. This is
# a killer feature over Makefiles.
#

# ensure dev requirements installed and up to date
devenv: prodenv requirements-dev && install-precommit
    #!/usr/bin/env bash
    # exit if .txt file has not changed since we installed them (-nt == "newer than', but we negate with || to avoid error exit code)
    test requirements.dev.txt -nt $VIRTUAL_ENV/.dev || exit 0

    $PIP install -r requirements.dev.txt
    touch $VIRTUAL_ENV/.dev

# ensure precommit is installed
install-precommit:
    #!/usr/bin/env bash
    BASE_DIR=$(git rev-parse --show-toplevel)
    test -f $BASE_DIR/.git/hooks/pre-commit || $BIN/pre-commit install

# upgrade dev or prod dependencies (specify package to upgrade single package, all by default)
upgrade env package="": virtualenv
    #!/usr/bin/env bash
    opts="--upgrade"
    test -z "{{ package }}" || opts="--upgrade-package {{ package }}"
    FORCE=true "{{ just_executable() }}" requirements-{{ env }} $opts

# *ARGS is variadic, 0 or more. This allows us to do `just test -k match`, for example.

# Run the tests
test *ARGS: devenv
    $BIN/python -m pytest {{ ARGS }}

test-fast *ARGS: devenv
    $BIN/python -m pytest tests -m "not slow_test" {{ ARGS }}

test-verbose *ARGS: devenv
    $BIN/python -m pytest tests/test_integration.py -o log_cli=true -o log_cli_level=INFO {{ ARGS }}

test-no-docker *ARGS: devenv
    $BIN/python -m pytest -m "not needs_docker" {{ ARGS }}

package-build: virtualenv
    rm -rf dist

    $PIP install build
    $BIN/python -m build

package-test type: package-build
    #!/usr/bin/env bash
    VENV="test-{{ type }}"
    distribution_suffix="{{ if type == "wheel" { "whl" } else { "tar.gz" } }}"

    # build a fresh venv
    python -m venv $VENV

    # ensure a modern pip
    $VENV/bin/pip install pip --upgrade

    # install the wheel distribution
    $VENV/bin/pip install dist/*."$distribution_suffix"

    # Minimal check that it has actually built correctly
    # Note: this uses the command installed into the virtualenv, not python -m,
    # to make sure we test the installed package and don't accidentally rely on
    # imports picking code on-disk.
    $VENV/bin/local_run --help

    # check we haven't packaged tests with it
    unzip -Z -1 dist/*.whl | grep -vq "^tests/"

    # clean up after ourselves
    rm -rf $VENV

# test the license shenanigins work when run from a console
test-stata: devenv
    rm -f tests/fixtures/stata_project/output/env.txt
    $BIN/python -c 'from jobrunner.cli.local_run import main; main("tests/fixtures/stata_project", ["stata"])'
    cat tests/fixtures/stata_project/output/env.txt
    echo

# runs the format (black), sort (isort) and lint (flake8) check but does not change any files
check: devenv
    $BIN/black --check .
    $BIN/isort --check-only --diff .
    $BIN/flake8

# fix formatting and import sort ordering
fix: devenv
    $BIN/black .
    $BIN/isort .
    just --fmt --unstable --justfile justfile
    just --fmt --unstable --justfile docker/justfile

# Run the dev project
run repo action: devenv
    $BIN/add_job {{ repo }} {{ action }}

# required by docker-compose.yaml
dotenv:
    cp dotenv-sample .env

update-wheels: devenv
    #!/bin/bash
    if test -d lib; then
        git -C lib pull
    else
        git clone git@github.com:opensafely/job-runner-dependencies.git lib
    fi
    $BIN/pip install -U -r requirements.prod.txt -r requirements.tools.txt --target lib
    cp requirements.prod.txt requirements.tools.txt lib/
    rm -rf lib/bin lib/*.dist-info
    rm lib/_ruamel_yaml.*.so
    rm lib/pydantic/.*.so
