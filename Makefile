.PHONY: venv
venv: venv/ready

venv/ready: requirements.dev.txt requirements.txt
	virtualenv venv -p python3.8
	venv/bin/pip install -r requirements.dev.txt
	touch $@

test: venv/ready
	$(PYTHON) -m pytest

test-fast: venv/ready
	$(PYTHON) -m pytest -m "not slow_test"

test-verbose: venv/ready
	$(PYTHON) -m pytest tests/test_integration.py -o log_cli=true -o log_cli_level=INFO

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
	$(PYTHON) -c 'from jobrunner.local_run import main; main("tests/fixtures/stata_project", ["stata"])'
	cat tests/fixtures/stata_project/output/env.txt
	echo
