#!/bin/bash
# Pull a docker image on the server
set -eu
if test "${1:-}" == ""; then
  echo "usage: $0 image[:tag]"
  exit 1
fi
set -x
docker pull docker-proxy.opensafely.org/opensafely-core/$1
docker tag docker-proxy.opensafely.org/opensafely-core/$1 ghcr.io/opensafely-core/$1
# temp b/w compat tag
docker tag docker-proxy.opensafely.org/opensafely-core/$1 ghcr.io/opensafely/$1

# If the image being updated is databuilder, make sure we get the aliased ehrql tag too
if [[ $1 == databuilder* ]]; then
  image=$(echo "$1" | sed -r 's/[databuilder]+/ehrql/g')
  echo "Updating ehrql image: $image"
  docker pull docker-proxy.opensafely.org/opensafely-core/$image
  docker tag docker-proxy.opensafely.org/opensafely-core/$image ghcr.io/opensafely-core/$image
fi

docker image prune --force
