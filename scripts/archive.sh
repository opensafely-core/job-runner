#!/bin/bash
set -euo pipefail

workspace=$1
workspace_dir=$HIGH_PRIVACY_STORAGE_BASE/workspaces/$workspace
archive=$HIGH_PRIVACY_STORAGE_BASE/archives/$workspace.tar.xz


if ! test -d "$workspace_dir"; then
    if test -f "$archive"; then
        echo "$workspace is already archived at $archive"
        exit 1
    else
        echo "Directory $workspace_dir does not exist"
        exit 1
    fi
fi

index=$(mktemp)
tar --directory "$HIGH_PRIVACY_STORAGE_BASE/workspaces" --create --xz --verbose --file "$archive" "$workspace/" | tee "$index"


# compare the list of files we expect to check that the tar seems good.
if ! diff -u "$index" <(tar --list --file "$archive"); then
    echo "$archive does not contain the expected list of files!"
    echo "Exiting *without* deleting $workspace_dir"
    exit 1
fi

read -p "$archive created. About to remove $workspace_dir directory. Are you sure? " -n 1 -r
if test "$REPLY" != "y"; then
    echo "Not removing $workspace_dir"
    exit 1
fi
echo

rm -rf "$workspace_dir"
