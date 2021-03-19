#!/bin/bash

# Finds the volume associated with a specific job and mounts it into a
# privileged container runnig our "tools" image for debugging.
#
# Any non-option argument is used as a substring search on the volume name.
# Otherwise all job-runner volumes are listed.

set -euo pipefail

# Pull out any arguments not starting with "-" and use them as a substring
# search on volume names.
name_search=''
for arg do
  shift
  case "$arg" in
    (-*) set -- "$@" "$arg" ;;
     (*) name_search="$arg"
  esac
done

# Other arguments are currently unsupported
if [[ -n "$@" ]]; then
  echo "Unsupported argument: $@"
  exit 1
fi

# An empty name search matches everything, which is what we want
volume_list="$(
  docker volume ls \
    --filter "name=$name_search" \
    --filter 'label=job-runner' \
    --format='{{.Name}}'
)"
volume_array=($volume_list)
volume_count=${#volume_array[@]}

if [[ -z "$name_search" ]]; then
  verb="job-runner"
else
  verb="matching"
fi

if [[ "$volume_count" == 0 ]]; then
    echo "No $verb volumes found"
    exit 1
fi

# If we supplied a search and there's exactly one match then select it
# automatically
if [[ -n "$name_search" && "$volume_count" == 1 ]]; then

    selected_volume=${volume_array[0]}

# Otherwise display a menu
else

  echo $"Found $volume_count $verb volumes:\n"
  PS3=$'\nEnter volme number: '
  select selected_volume in ${volume_array[@]}; do
    break
  done

fi

exec docker run --rm -it \
  --privileged --pid=host \
  --workdir //workspace \
  -v "$selected_volume:/workspace" \
  ghcr.io/opensafely-core/tools
