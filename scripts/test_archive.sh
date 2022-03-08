#!/bin/bash
set -euo pipefail

HIGH_PRIVACY_STORAGE_BASE=$(mktemp -d)
trap 'rm -rf $HIGH_PRIVACY_STORAGE_BASE' EXIT

error () {
    echo "$@"
    exit 1
}

mkdir -p "$HIGH_PRIVACY_STORAGE_BASE/archives"
DIR="$HIGH_PRIVACY_STORAGE_BASE/workspaces/test-workspace"
mkdir -p "$DIR"

echo "foo" > "$HIGH_PRIVACY_STORAGE_BASE/workspaces/test-workspace/foo.txt"
echo "bar,baz" > "$HIGH_PRIVACY_STORAGE_BASE/workspaces/test-workspace/bar.csv"

export HIGH_PRIVACY_STORAGE_BASE

EXPECTED_ARCHIVE="$HIGH_PRIVACY_STORAGE_BASE/archives/test-workspace.tar.xz"

echo y | ./scripts/archive.sh test-workspace

test -f "$EXPECTED_ARCHIVE" || error "Could not find $EXPECTED_ARCHIVE"
test -d "$DIR" && error "$DIR still exists"

echo y | ./scripts/unarchive.sh test-workspace

test -f "$EXPECTED_ARCHIVE" && error "$EXPECTED_ARCHIVE still exists"
test -d "$DIR" || error "$DIR does not exist"
