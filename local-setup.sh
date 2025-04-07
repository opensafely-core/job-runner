#!/bin/bash
set -euo pipefail

test -f .env|| cp dotenv_sample .env

mkdir -p workdir/high_privacy
mkdir -p workdir/medium_privacy

high_privacy_dir="$PWD/workdir/high_privacy"
med_privacy_dir="$PWD/workdir/medium_privacy"

sed -i"" -e "s|^HIGH_PRIVACY_STORAGE_BASE=.*|HIGH_PRIVACY_STORAGE_BASE=\"$high_privacy_dir\"|" .env
sed -i"" -e "s|^MEDIUM_PRIVACY_STORAGE_BASE=.*|MEDIUM_PRIVACY_STORAGE_BASE=\"$med_privacy_dir\"|" .env
