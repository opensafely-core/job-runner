# Deployment

This repository contains two services: the RAP Agent and the RAP
Controller. Both are deployed automatically on merge to main.


## Agent deployment

This runs within the secure backends (one instance per backend) and is
managed by the [backend-server](https://github.com/opensafely-core/backend-server/) repo.

A timer checks for new builds of the Docker image and pulls them down,
so there may be a short delay (ten minutes maybe?) between the Github
Actions workflows completing and the new image being deployed.


## Controller deployment

We run a single instance of the controller which all the agents connect
to. This is deployed as a Dokku app and at the time of writing runs on
`dokku4`. A Github Actions workflow connects to the host and runs the
dokku deploy process with the newly built image.

Below are the steps used to configure the app initially (though the
exact config should be regarded as illustrative and may well have
changed by the time of reading).

Assume we have a server correctly configured by the [sysadmin](https://github.com/bennettoxford/sysadmin)
tooling. This will ensure that Dokku is installed and configured with the
necessary plugins and that a `rap-controller` user has been created.

We also assume an appropriate "backups" volume has been configured and
mounted at `/mnt/volume_lon1_02` with regular snapshots externally
configured.

First we create the app and do some basic configuration:
```bash
dokku apps:create rap-controller

# Set up the domain
dokku domains:set rap-controller controller.opensafely.org
dokku letsencrypt:enable rap-controller

# Disable zero-downtime deploys for service, we don't ever want two of
# these loops running simultaneously
dokku checks:disable rap-controller service

# Create an appropriately owned storage directory and mount it
sudo mkdir -p /var/lib/dokku/data/storage/rap-controller
sudo chown rap-controller:rap-controller /var/lib/dokku/data/storage/rap-controller
dokku storage:mount rap-controller /var/lib/dokku/data/storage/rap-controller:/storage

# Mount the backups directory
dokku storage:mount rap-controller /mnt/volume_lon1_02/backups/rap-controller:/backups

# Run app as correct user, both the deployed services and one-off commands
dokku docker-options:add rap-controller deploy,run "--user=$(id -u rap-controller):$(id -g rap-controller)"
```

Then we need to define some sensitive config values (note the leading
space to exclude from bash history):
```bash
  secrets=(
  # If re-deploying just generate a new one of these with:
  #   head -c 32 /dev/urandom | base64
  DJANGO_CONTROLLER_SECRET_KEY='[XXXXX]'

  # Get these from https://jobs.opensafely.org/staff/backends/
  TPP_JOB_SERVER_TOKEN='[XXXXX]'
  TEST_JOB_SERVER_TOKEN='[XXXXX]'

  # These allow job-server to call the RAP API; they should correspond to job-server's `RAP_API_TOKEN`
  TEST_CLIENT_TOKENS='[XXXXX]'
  TPP_CLIENT_TOKENS='[XXXXX]'

  # This is a token called `rap-controller-token` belonging to the `opensafely-readonly`
  # Github user. The token is not stored anywhere else but login details for the user
  # are in Bitwarden so the token can be regenerated.
  PRIVATE_REPO_ACCESS_TOKEN='[XXXXX]'
  # This is stored in Bitwarden
  STATA_LICENSE='[XXXXX]'

  # Use the "jobrunner" key from:
  # https://ui.honeycomb.io/bennett-institute-for-applied-data-science/environments/production/api_keys
  OTEL_EXPORTER_OTLP_HEADERS='x-honeycomb-team=[XXXXX]'
)
```

Then some non-sensitive config:
```bash
config=(
  DJANGO_DEBUG=False
  DJANGO_CONTROLLER_ALLOWED_HOSTS=controller.opensafely.org
  WORKDIR=/storage
  BACKUPS_PATH=/backups

  # Comma-separated list of backends that the controller manages; these correspond to Backend slugs in job-server
  BACKENDS=tpp,test

  # Loop timings taken from current TPP backend settings
  JOB_LOOP_INTERVAL=5.0

  # TPP specific config taken from current backend settings
  TPP_MAX_WORKERS=15
  TPP_MAX_DB_WORKERS=6
  TPP_JOB_CPU_COUNT=4
  TPP_JOB_MEMORY_LIMIT=128G

  # Service specific honeycomb dataset name
  OTEL_SERVICE_NAME=rap-controller
)
```

Finally we can apply the configuration and deploy the app:
```bash
dokku config:set rap-controller "${config[@]}" "${secrets[@]}"
dokku git:from-image rap-controller ghcr.io/opensafely-core/job-runner:latest
```


## Restoring database backups

Log in to Digital Ocean and navigate to "Backups & Snapshots >
Snapshots > Volumes":
https://cloud.digitalocean.com/images/snapshots/volumes

You should see (among other things) a number of timestamped SnapShooter snapshots of
`volume-lon1-02`. Find the one you to want to restore from and click
"More > Create Volume".

You can then follow the standard instructions for attaching this volume
to a Droplet and mounting it. Most likely you'll want to attach it to
the Droplet running the RAP Controller, but it doesn't have to be.

Once the volume is mounted you should be able to extract the single,
timestamped SQLite file from it. If you don't know what to do with this
file then you're probably not the person to be doing the restore :)

Don't forget to unmount, detatch and destroy the volume afterwards so we
don't pay for it indefinitely.
