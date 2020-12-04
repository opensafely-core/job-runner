#!/bin/bash
# Pull a docker image on the server
set -eu
if test "${1:-}" == ""; then
  echo "usage: $0 image[:tag]"
  exit 1
fi
set -x
docker pull docker-proxy.opensafely.org/opensafely/$1
docker tag docker-proxy.opensafely.org/opensafely/$1 ghcr.io/opensafely/$1
