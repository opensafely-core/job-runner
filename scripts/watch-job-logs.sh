#!/bin/bash

# Wrapper around `docker logs` which searches for job containers (using the
# label set by the job-runner) and displays a menu of them. When the user
# selects a job it displays its log output.
#
# Any non-option argument is used as a substring search on the job name. All
# other arguments are passed to `docker logs`.

set -euo pipefail

# Pull out any arguments not starting with "-" and use them as a substring
# search on container names. All remaining arguments are passed to `docker
# logs`
name_search=''
for arg do
  shift
  case "$arg" in
    (-*) set -- "$@" "$arg" ;;
     (*) name_search="$arg"
  esac
done

# An empty name search matches everything, which is what we want
job_list="$(
  docker container ls --all \
    --filter 'label=jobrunner-job' \
    --filter "name=$name_search" \
    --format='{{.Names}}'
)"
job_array=($job_list)
job_count=${#job_array[@]}

if [[ -z "$name_search" ]]; then
  verb="running"
else
  verb="matching"
fi

if [[ "$job_count" == 0 ]]; then
    echo "No $verb jobs found"
    exit 1
fi

# If we supplied a search and there's exactly one match then select it
# automatically
if [[ -n "$name_search" && "$job_count" == 1 ]]; then

    selected_job=${job_array[0]}

# Otherwise display a menu
else

  echo $"Found $job_count $verb jobs:\n"
  PS3=$'\nEnter job number: '
  select selected_job in ${job_array[@]}; do
    break
  done

fi

exec docker logs "$selected_job" --follow "$@"
