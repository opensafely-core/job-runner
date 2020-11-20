#!/bin/bash
set -eo pipefail

if [[ "$OS" != "Windows_NT" ]]; then
  echo "This script is intended to be run inside git-bash on Windows"
  exit 1
fi

venv_dir="$PWD/venv"

if [[ ! -d "$venv_dir" ]]; then
  echo "Creating virtualenv in $venv_dir"
  python -m venv "$venv_dir"
fi

. "$venv_dir/Scripts/activate"

install_flag="$venv_dir/installed.txt"
if [[ ! -f "$install_flag" ]]; then
  echo "Installing requirements"
  pip install -r requirements.dev.txt
  touch "$install_flag"
fi

docker_cli_url="https://github.com/StefanScherer/docker-cli-builder/releases/download/19.03.12/docker.exe"
if ! which docker >/dev/null 2>&1; then
  docker_cli="$venv_dir/Scripts/docker.exe"
  if [[ ! -f "$docker_cli" ]]; then
    echo "Downloading docker cli"
    curl -L "$docker_cli_url" -o "$docker_cli.tmp"
    mv "$docker_cli.tmp" "$docker_cli"
  fi
fi

export DOCKER_HOST=tcp://10.0.2.2:8344
exec python -m pytest "$@"
