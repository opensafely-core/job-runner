# syntax=docker/dockerfile:1.2
#################################################
#
# Create base image with python installed.
#
# DL3007 ignored because base-docker we specifically always want to build on
# the latest base image, by design.
#
# hadolint ignore=DL3007
FROM ghcr.io/opensafely-core/base-docker:latest as base-python

# we are going to use an apt cache on the host, so disable the default debian
# docker clean up that deletes that cache on every apt install
RUN rm -f /etc/apt/apt.conf.d/docker-clean

# ensure fully working base python3 installation
# see: https://gist.github.com/tiran/2dec9e03c6f901814f6d1e8dad09528e
# use space efficient utility from base image
RUN --mount=type=cache,target=/var/cache/apt \
  /root/docker-apt-install.sh python3 python3-venv python3-pip python3-distutils tzdata ca-certificates

# install any additional system dependencies
COPY docker/dependencies.txt /tmp/dependencies.txt
RUN --mount=type=cache,target=/var/cache/apt \
    /root/docker-apt-install.sh /tmp/dependencies.txt


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
FROM base-python as builder

# Install any system build dependencies
COPY docker/build-dependencies.txt /tmp/build-dependencies.txt
RUN --mount=type=cache,target=/var/cache/apt \
    /root/docker-apt-install.sh /tmp/build-dependencies.txt

# Install everything in venv for isolation from system python libraries
RUN python3 -m venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv/ PATH="/opt/venv/bin:$PATH"

# The cache mount means a) /root/.cache is not in the image, and b) it's preserved
# between docker builds locally, for faster dev rebuild.
COPY requirements.txt /tmp/requirements.prod.txt

# DL3042: using cache mount instead
# DL3013: we always want latest pip/setuptools/wheel, at least for now
# hadolint ignore=DL3042,DL3013
RUN --mount=type=cache,target=/root/.cache \
    python -m pip install -U pip setuptools wheel && \
    python -m pip install --requirement /tmp/requirements.prod.txt


##################################################
#
# Base project image
#
# Ok, we've built everything we need, build an image with all dependencies but
# no code.
#
# Not including the code at this stage has two benefits:
#
# 1) this image only rebuilds when the handfull of files needed to build job-runner-base
#    changes. If we do `COPY . /app` now, this will rebuild when *any* file changes.
#
# 2) Ensures we *have* to mount the volume for dev image, as there's no embedded
#    version of the code. Otherwise, we could end up accidentally using the
#    version of the code included when the prod image was built.
FROM base-python as job-runner-base

# Create a non-root user to run the app as
# We currently use host bind mounts in production backends, which means if we
# need to match the uid inside the container to the one on the host.  To do
# this, we reserve uid/gid 10000 to use as our running user on the host.
RUN useradd --create-home --user-group --uid 10000 appuser

# copy venv over from builder image. These will have root:root ownership, but
# are readable by all.
COPY --from=builder /opt/venv /opt/venv

# Ensure we're using the venv by default
ENV VIRTUAL_ENV=/opt/venv/ PATH="/opt/venv/bin:$PATH"

RUN mkdir /app
WORKDIR /app
VOLUME /app

# We set command rather than entrypoint, to make it easier to run different
# things from the cli
CMD ["/opt/venv/bin/python", "-m", "jobrunner.service"]

# This may not be necessary, but it probably doesn't hurt
ENV PYTHONPATH=/app

# switch to running as the user
USER appuser


##################################################
#
# Production image
#
# Copy code in, add proper metadata
FROM job-runner-base as job-runner-prod

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



##################################################
#
# Dev image
#
# Now we build a dev image from our job-runner-dev image. This is basically
# installing dev dependencies and matching local UID/GID. It is expected that
# the current code will be mounted in /app when this is run
#
FROM job-runner-base as job-runner-dev

# switch back to root to run the install of dev requirements.txt
USER root

# TODO: its possible python dev dependencies might need some additional build packages installed?

# install development requirements
COPY requirements.dev.txt /tmp/requirements.dev.txt
# using cache mount instead
# hadolint ignore=DL3042
RUN --mount=type=cache,target=/root/.cache \
    python -m pip install --requirement /tmp/requirements.dev.txt

# in dev, ensure appuser uid matches host user id
ARG USERID=1000
ARG GROUPID=1000
RUN usermod -u $USERID appuser && groupmod -g $GROUPID appuser

# switch back to appuser
USER appuser

##################################################
#
# Graphnet image
#
# Copy code in, add proper metadata
FROM job-runner-base as job-runner-tools-graphnet

# Adjust this metadata to fit project. Note that the base-docker image does set
# some basic metadata.
LABEL org.opencontainers.image.title="job-runner-tools-graphnet" \
      org.opencontainers.image.description="Image of Prepare and Finalize for the OpenSAFELY secure job-runner service" \
      org.opencontainers.image.source="https://github.com/opensafely-core/job-runner"

# copy application code
COPY /jobrunner/lib/. /app/jobrunner/lib
COPY /jobrunner/__init__.py /app/jobrunner/__init__.py
COPY /jobrunner/config.py /app/jobrunner/config.py
COPY /jobrunner/executors/graphnet/container/prepare.py /app/jobrunner/prepare.py
COPY /jobrunner/executors/graphnet/container/finalize.py /app/jobrunner/finalize.py

# finally, tag with build information. These will change regularly, therefore
# we do them as the last action.
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.created=$BUILD_DATE
ARG GITREF=unknown
LABEL org.opencontainers.image.revision=$GITREF

## changed to root in order to manage the files in the PV in k8s
USER root
