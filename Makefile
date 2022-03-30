# remove default Makefile rules
MAKEFLAGS += --no-builtin-rules
MAKEFLAGS += --no-builtin-variables
.SUFFIXES:


.PHONY: venv
venv: venv/ready

venv/ready: requirements.dev.txt requirements.txt
	virtualenv venv -p python3.8
	venv/bin/pip install -r requirements.dev.txt
	touch $@

test: venv/ready
	venv/bin/python -m pytest tests

test-fast: venv/ready
	venv/bin/python -m pytest tests -m "not slow_test"

test-verbose: venv/ready
	venv/bin/python -m pytest tests/test_integration.py -o log_cli=true -o log_cli_level=INFO

run: venv/ready
	venv/bin/add_job $(REPO) $(ACTION)

lib:
	git clone git@github.com:opensafely/job-runner-dependencies.git lib


requirements%.txt: requirements%.in
	venv/bin/pip-compile $<


update-wheels: venv/ready requirements.txt requirements.tools.txt | lib
	#git -C lib pull
	venv/bin/pip install -r requirements.txt -r requirements.tools.txt --target lib
	cp requirements.txt requirements.tools.txt lib/
	rm -rf lib/bin lib/*.dist-info
	rm lib/_ruamel_yaml.*.so


# test the license shenanigins work when run from a console
test-stata: venv/ready
	rm -f tests/fixtures/stata_project/output/env.txt
	venv/bin/python -c 'from jobrunner.cli.local_run import main; main("tests/fixtures/stata_project", ["stata"])'
	cat tests/fixtures/stata_project/output/env.txt
	echo


# include docker commands in main Makefile
# Assumption is that this will be replaced by justfile at some point
docker-build docker-serve docker-run docker-test docker-clean: .env
	$(MAKE) -C docker "$@"


# required by docker-compose.yaml
.env:
	cp dotenv-sample .env

fix:
	venv/bin/black jobrunner tests
	venv/bin/isort jobrunner tests
