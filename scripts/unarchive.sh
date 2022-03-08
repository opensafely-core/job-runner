#!/bin/bash
set -euo pipefail

workspace=$1
workspace_dir=$HIGH_PRIVACY_STORAGE_BASE/workspaces/$workspace
archive=$HIGH_PRIVACY_STORAGE_BASE/archives/$workspace.tar.xz


if ! test -f "$archive"; then
    if test -d "$workspace_dir"; then
        echo "$workspace_dir already exists"
        exit 1
    else
        echo "Archive file $archive does not exist"
        exit 1
    fi
fi

tar --directory "$HIGH_PRIVACY_STORAGE_BASE/workspaces" --extract --xz --verbose --file "$archive" 

read -p "$workspace_dir created from $archive. About to remove $archive. Are you sure? " -n 1 -r
if test "$REPLY" != "y"; then
    echo "Not removing $archive"
    exit 1
fi
echo

rm -rf "$archive"
