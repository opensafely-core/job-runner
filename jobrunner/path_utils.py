import os


def list_dir_with_ignore_patterns(directory, ignore_patterns):
    matches = []
    for pattern in ignore_patterns:
        matches.extend(directory.glob(escape_globs(pattern)))
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


def escape_globs(pattern):
    # TODO
    return pattern
