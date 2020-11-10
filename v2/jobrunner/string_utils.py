import re


def slugify(s):
    s = s.encode("ascii", "ignore").decode("ascii").lower()
    # Remove anything that's not alphanumeric, underscore, dash or whitespace
    s = re.sub(r"[^\w\s\-]", "", s)
    # Replace repeated runs of anything non-alphanumeric with a single dash
    return re.sub(r"[_\s\-]+", "-", s).strip("-")
