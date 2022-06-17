import os


def list_dir_with_ignore_patterns(directory, ignore_patterns):
    """
    Given a directory and list of glob patterns, return all paths in that
    directory which don't match any of the glob patterns.

    Note that this function won't descend further than it needs to so, for
    instance, if there is a top level directory "foo" and no ignore pattern
    begins with "foo/" then it will return "foo" without iterating any of the
    files or sub-directories within it.
    """
    matches = []
    for pattern in ignore_patterns:
        matches.extend(directory.glob(pattern))
    match_tree = {}
    for match in matches:
        relative_match = match.relative_to(directory)
        tree = match_tree
        for segment in str(relative_match).split(os.sep):
            if segment not in tree:
                tree[segment] = {}
            tree = tree[segment]
    return [path.relative_to(directory) for path in _iter_dir(directory, match_tree)]


def _iter_dir(directory, match_tree):
    for item in directory.iterdir():
        subtree = match_tree.get(item.name)
        # No match: yield this file
        if subtree is None:
            yield item
        # Match is a leaf node: ignore this specific file
        elif not subtree:
            pass
        # Otherwise filter this subdirectory using the subtree
        else:
            yield from _iter_dir(item, subtree)


def ensure_unix_path(path):
    "Ensure path is unix path string"
    # if we're string, handle it as a string
    if isinstance(path, str):
        return path.replace("\\", "/")
    return path.as_posix()
