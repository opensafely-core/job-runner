import re


def slugify(s):
    s = s.encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"[^\w\s\-]", "", s)
    return re.sub(r"[_\s\-]+", "-", s).strip("-")
