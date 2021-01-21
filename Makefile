VENV_DIR ?= .venv
VENV ?= $(VENV_DIR)/done
PYTHON ?= $(VENV_DIR)/bin/python

$(VENV): requirements.dev.txt requirements.txt
	virtualenv .venv -p python3.8
	$(VENV_DIR)/bin/pip install -r requirements.dev.txt
	touch $@

test: $(VENV)
	$(PYTHON) -m pytest

test-fast: $(VENV)
	$(PYTHON) -m pytest -m "not slow_test"

test-verbose: $(VENV)
	$(PYTHON) -m pytest tests/test_integration.py -o log_cli=true -o log_cli_level=INFO

run: $(VENV)
	$(PYTHON) -m jobrunner.add_job $(REPO) $(ACTION)

lib:
	git clone git@github.com:opensafely/job-runner-dependencies.git lib


requirements%.txt: requirements%.in
	$(VENV_DIR)/bin/pip-compile $<
    

update-wheels: $(VENV) requirements.txt requirements.tools.txt | lib
	#git -C lib pull
	$(VENV_DIR)/bin/pip install -r requirements.txt -r requirements.tools.txt --target lib
	cp requirements.txt requirements.tools.txt lib/
	rm -rf lib/bin lib/*.dist-info
	rm lib/_ruamel_yaml.*.so
        

# test the license shenanigins work when run from a console
test-stata: $(VENV)
	rm -f tests/fixtures/stata_project/output/env.txt
	$(PYTHON) -c 'from jobrunner.local_run import main; main("tests/fixtures/stata_project", ["stata"])'
	cat tests/fixtures/stata_project/output/env.txt
	echo
