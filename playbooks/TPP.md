# TPP backend

Use git-bash as your terminal

Service location: /e/job-runner

## Operations

### Starting/stopping the service

    docker-compose up -d

    docker-compose stop

### Viewing logs

    docker-compose logs -t | less -R

### Update docker image

    ./scripts/update-docker-image.sh image[:tag]

### View specific job logs

    /e/bin/watch-job-logs.sh

This will let you choose a job's output to tail


