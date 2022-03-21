#!/bin/bash
set -euo pipefail

workspace=$1
workspace_dir=$HIGH_PRIVACY_STORAGE_BASE/workspaces/$workspace
archive=$HIGH_PRIVACY_STORAGE_BASE/archives/$workspace.tar.xz
tmp_archive=$archive.tmp

if ! test -d "$workspace_dir"; then
    if test -f "$archive"; then
        echo "$workspace is already archived at $archive"
        exit 0
    else
        echo "Directory $workspace_dir does not exist"
        exit 0
    fi
elif test -f "$archive"; then
    echo "Both $archive and $workspace_dir exist!"
    exit 1
fi

index=$(mktemp)

echo "before: $(du -sh "$workspace_dir")"
tar --directory "$HIGH_PRIVACY_STORAGE_BASE/workspaces" --create --gzip --verbose --file "$tmp_archive" "$workspace/" | tee "$index"
echo "after: $(du -sh "$tmp_archive")"


# compare the list of files we expect to check that the tar seems good.
if ! diff -u "$index" <(tar --list --file "$archive"); then
    echo "$archive does not contain the expected list of files!"
    echo "Exiting *without* deleting $workspace_dir"
    rm "$tmp_archive"
    exit 1
else
    mv "$tmp_archive" "$archive"
fi

read -p "$archive created. About to remove $workspace_dir directory. Are you sure? " -n 1 -r
if test "$REPLY" != "y"; then
    echo "Not removing $workspace_dir"
    exit 1
fi
echo

rm -r "$workspace_dir"
# on TPP windows this needs to be docker 
#docker run --rm --volume //e://e --entrypoint rm ghcr.io/opensafely-core/cohortextractor" -r "/$workspace_dir"
