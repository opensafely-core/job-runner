#!/bin/bash

source .env.graphnet

# run separately:
python -m jobrunner.sync
python -m jobrunner.run
