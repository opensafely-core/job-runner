# used to ensure the dev container uses the same uid/gid as the current user
export DOCKER_USERID ?= $(shell id -u)
export DOCKER_GROUPID ?= $(shell id -g)

# user to ensure that ssh access is set up to the host
export DOCKER_USER ?= $(USER)
export DOCKER_ADDR ?= $(shell docker network inspect bridge --format='{{(index .IPAM.Config 0).Gateway}}')

SSH_KEY=ssh/id_ed25519
SSH_PUBKEY=$(SSH_KEY).pub
SSH_COMMENT=local jobrunner dev key

# create known_hosts file so ssh works without prompting
ssh/known_hosts:
	ssh-keyscan $(DOCKER_ADDR) > ssh/known_hosts


# create a local dev key with no password to use for ssh access
$(SSH_KEY):
	ssh-keygen -t ed25519 -N '' -C "$(SSH_COMMENT)" -f $@



.PHONY: docker-github-actions
enable-github-actions-ssh:
	chmod 700 ~
	mkdir ~/.ssh -m 700
	touch ~/.ssh/authorized_keys
	chmod 600 ~/.ssh/authorized_keys


# ensure the local dev key is allowed to ssh in as current user
$(SSH_KEY).authorized: $(SSH_KEY) ssh/known_hosts
#ifeq ($(CI),"true")
    # ensure local user is set up for ssh in GHA
	chmod 700 ~
	mkdir ~/.ssh -m 700
	touch ~/.ssh/authorized_keys
	chmod 600 ~/.ssh/authorized_keys
#endif
	grep -q "$(shell cat $(SSH_PUBKEY))" ~/.ssh/authorized_keys || cat $(SSH_PUBKEY) >> ~/.ssh/authorized_keys
#ifeq ($(CI),"true")
	# test ssh works
	ssh -i $(SSH_KEY) -o UserKnownHostsFile=ssh/known_hosts $(DOCKER_USER)@$(DOCKER_ADDR) true || { tail /var/log/auth.log; exit 1; }
#endif
	touch $@


.PHONY: docker-build
# enable modern docker build features
docker-build: export DOCKER_BUILDKIT=1
docker-build: export COMPOSE_DOCKER_CLI_BUILD=1
# set build args for prod builds
docker-build: export BUILD_DATE=$(date -u +'%y-%m-%dT%H:%M:%SZ')
docker-build: export GITREF=$(git rev-parse --short HEAD)
docker-build: ENV ?= dev
docker-build:
	docker-compose build --pull $(ENV) 


.PHONY: docker-test
# run tests in docker container
docker-test: docker-build $(SSH_KEY).authorized
	docker-compose run $(ARGS) --rm test


.PHONY: docker-serve
# run dev server in docker container
docker-serve: docker-build $(SSH_KEY).authorized
	docker-compose up dev


.PHONY: docker-run
# run command in dev container
docker-run: CMD ?= bash
docker-run: docker-build $(SSH_KEY).authorized
	docker-compose run --rm $(ARGS) dev $(CMD)


.PHONY: docker-exec
# exec command in existing dev container
docker-exec: CMD ?= bash
docker-exec: docker-build $(SSH_KEY).authorized
	docker-compose exec dev $(CMD)


.PHONY: docker-clean
docker-clean:
	# clean up local ssh config
	rm -f ssh/known_hosts ssh/id_ed25519*
	sed -i '/$(SSH_COMMENT)/d' ~/.ssh/authorized_keys
