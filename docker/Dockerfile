# syntax=docker/dockerfile:1
# check=error=true
#################################################
#
# Images are structured as shown in this diagram:
# ┌──────────────────┐            ┌──────────────────┐
# │   base-python    ├────────────►     builder      │
# └────────┬─────────┘            └──────────┬───────┘
#          │                                 │
#          │                                 │COPY FROM
#          │     ┌──────────────────┐        │
#          └─────► new-project-base ◄────────┘
#                └────────┬─┬───────┘
#                         │ │
#                         │ │
# ┌──────────────────┐    │ │     ┌──────────────────┐
# │ new-project-prod ◄────┘ └─────► new-project-dev  │
# └──────────────────┘            └──────────────────┘
# The goal of this stages structure is to a) minimise the size of the *prod* image by
# not including build dependencies and b) to make rebuilding the *dev* fast in local
# development, by avoiding invalidating the layer cache every time we change python
# code.
#
#################################################
#
# Create base image with python installed.
# All Dockerfiles should start from the base-docker image
#
# These build args have defaults, but are usually set from docker/.env. They
# are used in FROM lines, so need to be declared globally and have defaults
# set.
ARG UBUNTU_VERSION=22.04
ARG UV_VERSION=0.9

# DL3007 ignored because base-docker we specifically always want to build on
# the latest base image, by design.
#
# hadolint ignore=DL3007
FROM ghcr.io/opensafely-core/base-docker:${UBUNTU_VERSION} AS base-python

# we are going to use an apt cache on the host, so disable the default debian
# docker clean up that deletes that cache on every apt install
RUN rm -f /etc/apt/apt.conf.d/docker-clean

# use deadsnakes ppa to install a fully working base python installation
# see: https://gist.github.com/tiran/2dec9e03c6f901814f6d1e8dad09528e
# use space efficient utility from base image
RUN --mount=type=cache,target=/var/cache/apt <<'EOF'
UBUNTU_CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")
KEY_PATH=/usr/share/keyrings/deadsnakes.asc
KEY_URL='https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xf23c5a6cf475977595c89f51ba6932366a755776'
/usr/lib/apt/apt-helper download-file "$KEY_URL" "$KEY_PATH"
echo "deb [signed-by=$KEY_PATH] https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu ${UBUNTU_CODENAME} main" > /etc/apt/sources.list.d/deadsnakes-ppa.list
EOF

# install any additional system dependencies
# NOTE: ensure .python-version is not excluded in .dockerignore
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=bind,source=docker/dependencies.txt,target=/dependencies.txt \
    --mount=type=bind,source=.python-version,target=/.python-version \
    /root/docker-apt-install.sh /dependencies.txt "python$(cat /.python-version)"

# uv env var documentation: https://docs.astral.sh/uv/reference/environment/
# copy files rather than symlink since the cache and the target are on different filesystems
ENV UV_LINK_MODE=copy
# compile at installation time, not import time
ENV UV_COMPILE_BYTECODE=1
# we are providing python via deadsnakes ppa
# (as we require dynamic linking of openssl for security reasons)
ENV UV_PYTHON_DOWNLOADS=never
# set the directory for the venv
ENV UV_PROJECT_ENVIRONMENT="/opt/venv"

##################################################
#
# Reusable uv CLI layer
#
# This allows us to specify the version in one place, and also works around
# a limitation that you cannot use build args directly in `COPY --from`.
#
# See: https://github.com/moby/buildkit/issues/2412 for more infor
#
#
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv-cli

##################################################
#
# Build image
#
# Ok, now we have local base image with python and our system dependencies on.
# We'll use this as the base for our builder image, where we'll build and
# install any python packages needed.
#
# We use a separate, disposable build image to avoid carrying the build
# dependencies into the production image.
FROM base-python AS builder

# Install any system build dependencies
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=bind,source=docker/dependencies-build.txt,target=/dependencies-build.txt \
    /root/docker-apt-install.sh /dependencies-build.txt

# Pull in the binaries from our pinned uv-cli stage
COPY --from=uv-cli /uv /uvx /usr/local/bin/

RUN uv venv

# The cache mount means a) /root/.cache is not in the image, and b) it's preserved
# between docker builds locally, for faster dev rebuild.
# Setting --directory /root lets `uv` detect the pyproject.toml, uv.lock etc.
RUN --mount=type=cache,target=/root/.cache \
    --mount=type=bind,source=pyproject.toml,target=/root/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/root/uv.lock \
    uv sync --frozen --no-dev --no-install-project --directory /root

##################################################
#
# Base project image
#
# Ok, we've built everything we need, build an image with all dependencies but
# no code.
#
# Not including the code at this stage has two benefits:
#
# 1) this image only rebuilds when the handlful of files needed to build
#    the base project image changes. If we do `COPY . /app` now, this will
#    rebuild when *any* file changes.
#
# 2) Ensures we *have* to mount the volume for dev image, as there's no embedded
#    version of the code. Otherwise, we could end up accidentally using the
#    version of the code included when the prod image was built.
FROM base-python AS job-runner-base

RUN mkdir -p /app
WORKDIR /app
ENV VIRTUAL_ENV=/opt/venv/ \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app

# copy venv over from builder image. These will have root:root ownership, but
# are readable by all.
COPY --from=builder /opt/venv /opt/venv

# We use --init to run the docker container, which mounts tini binary, and uses
# it as PID 1, but only in single-process mode. Setting TINI_KILL_PROCESS_GROUP
# means it will forward signals to any background processes as well as the main
# process.
ENV TINI_KILL_PROCESS_GROUP=1

# Copy django bash completion script
COPY docker/scripts/django_bash_completion /etc/bash_completion.d
COPY docker/scripts/bashrc_addendum /tmp

# Add bash completion to bashrc
RUN cat /tmp/bashrc_addendum >> /etc/bash.bashrc && rm /tmp/bashrc_addendum

##################################################
#
# Production image
#
# Copy code in, add proper metadata
FROM job-runner-base AS job-runner-prod

# Adjust this metadata to fit project. Note that the base-docker image does set
# some basic metadata.
LABEL org.opencontainers.image.title="job-runner" \
      org.opencontainers.image.description="OpenSAFELY secure job-runner service" \
      org.opencontainers.image.source="https://github.com/opensafely-core/job-runner"

# copy application code
COPY . /app

# finally, tag with build information. These will change regularly, therefore
# we do them as the last action.
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.created=$BUILD_DATE
ARG GITREF=unknown
LABEL org.opencontainers.image.revision=$GITREF
RUN mkdir -p /app/metadata \
    && echo "${GITREF}" > /app/metadata/version.txt

##################################################
#
# Dev image
#
# Now we build a dev image from our job-runner-dev image. This is basically
# installing dev dependencies and matching local UID/GID. It is expected that
# the current code will be mounted in /app when this is run
#
FROM job-runner-base AS job-runner-dev

# Pull in the binaries from our pinned uv-cli stage
COPY --from=uv-cli /uv /uvx /bin/

# install development requirements
# we want to additionally sync dev dependencies into the venv;
# prod dependencies are already installed and we don't want to remove them.
RUN --mount=type=cache,target=/root/.cache \
    --mount=type=bind,source=pyproject.toml,target=/root/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/root/uv.lock \
    uv sync --frozen --no-install-project --directory /root
