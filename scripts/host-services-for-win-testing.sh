#!/bin/bash

set -eo pipefail

if [[ "$OS" == "Windows_NT" ]]; then
  echo "This script is intended to be run on the VM host"
  exit 1
fi

# Kill daemons when process exits
trap 'trap - SIGTERM && kill -- -$$' SIGINT SIGTERM EXIT

# Serve the git directory so we can easily get code into the VM without pushing
# to Github
GIT_PORT=8343
echo "Serving git directory $PWD on port $GIT_PORT"
git daemon --verbose --listen=127.0.0.1 --port="$GIT_PORT" --reuseaddr --export-all --base-path=. &

# Expose the Docker socket on a TCP port so the client inside the VM can talk
# to it
DOCKER_PORT=8344
echo "Exposing Docker daemon on port $DOCKER_PORT"
socat -d TCP4-LISTEN:"$DOCKER_PORT",fork,reuseaddr,bind=127.0.0.1 UNIX-CONNECT:/var/run/docker.sock &

wait
