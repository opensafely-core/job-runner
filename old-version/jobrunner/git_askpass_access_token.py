#!/usr/bin/env python
"""
Minimal askpass implementation that can be supplied as the value of GIT_ASKPASS
and will return the access token as the username when invoked. This allows us
to avoid embedding the token in the repo URL (where it leaks easily) or writing
it to disk.
"""
import os, sys

if __name__ == "__main__":
    if sys.argv[1].startswith("Username"):
        print(os.environ.get("PRIVATE_REPO_ACCESS_TOKEN", ""))
