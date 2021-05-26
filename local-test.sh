#!/bin/bash

set -a
source .env.local
set +a

# run separately:
python -m jobrunner.sync
python -m jobrunner.run
