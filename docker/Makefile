
# local dev env file for docker-compose to use when building
.env:
	cp ../dotenv-sample ../.env
	@echo "Generating local docker-compose .env file..."
	@echo "# local dev docker-compose config" > $@
	@echo "DEV_USERID=$(shell id -u)" >> $@
	@echo "DEV_GROUPID=$(shell id -g)" >> $@
	@echo "DOCKER_HOST_GROUPID=$(shell getent group docker | awk -F: '{print $$3}')" >> $@


.PHONY: docker-build
# enable modern docker build features
docker-build: export DOCKER_BUILDKIT=1
docker-build: export COMPOSE_DOCKER_CLI_BUILD=1
# set build args for prod builds
docker-build: export BUILD_DATE=$(date -u +'%y-%m-%dT%H:%M:%SZ')
docker-build: export GITREF=$(git rev-parse --short HEAD)
docker-build: ENV ?= dev
docker-build: ARGS ?= --pull
docker-build: .env
	docker-compose --env-file .env build  $(ARGS) $(ENV) 


.PHONY: docker-test
# run tests in docker container
docker-test: docker-build
	docker-compose run $(ARGS) --rm test


.PHONY: docker-serve
# run server in docker container
docker-serve: ENV ?= dev
docker-serve: docker-build
	docker-compose up $(ENV)


.PHONY: docker-run
# run command in container
docker-run: ENV ?= dev
docker-run: CMD ?= bash
docker-run: docker-build
	docker-compose run --rm $(ARGS) $(ENV) $(CMD)


.PHONY: docker-exec
# exec command in existing dev container
docker-exec: ENV ?= dev
docker-exec: CMD ?= bash
docker-exec: docker-build
	docker-compose exec $(ENV) $(CMD)


.PHONY: docker-clean
docker-clean:
	rm -f .env
	docker image rm job-runner job-runner-dev || true
	# clean up local ssh config
	rm -f ssh/known_hosts ssh/id_ed25519*
	sed -i '/$(SSH_COMMENT)/d' ~/.ssh/authorized_keys


# support using docker over ssh in dev
SSH_KEY ?= ssh/id_ed25519
SSH_PUBKEY ?= $(SSH_KEY).pub
SSH_COMMENT = local jobrunner dev key
SSH_USER ?= $(USER)
SSH_HOST ?= $(shell docker network inspect bridge --format='{{(index .IPAM.Config 0).Gateway}}')


# create known_hosts file so ssh works without prompting
ssh/known_hosts:
	ssh-keyscan $(SSH_HOST) > ssh/known_hosts


# create a local dev key with no password to use for ssh access
$(SSH_KEY):
	ssh-keygen -t ed25519 -N '' -C "$(SSH_COMMENT)" -f $@


# ensure the local dev key is allowed to ssh in as current user
$(SSH_KEY).authorized: $(SSH_KEY) ssh/known_hosts
	grep -q "$(shell cat $(SSH_PUBKEY))" ~/.ssh/authorized_keys || cat $(SSH_PUBKEY) >> ~/.ssh/authorized_keys
	# quick test to fail early if ssh doesn't work for some reason
	ssh -i $(SSH_KEY) -o UserKnownHostsFile=ssh/known_hosts $(SSH_USER)@$(SSH_HOST) true || { echo "Failed to ssh into $(SSH_HOST)"; tail /var/log/auth.log; exit 1; }
	touch $@


# this will override the default of /var/run/docker.sock
enable-docker-over-ssh: $(SSH_KEY).authorized .env
	@echo "Enabling use of SSH in .env..."
	@echo "DOCKER_HOST=ssh://$(SSH_USER)@$(SSH_HOST)" >> .env


# restore regular socket based docker access
disable-docker-over-ssh: .env
	rm -f .env
	$(MAKE) .env
